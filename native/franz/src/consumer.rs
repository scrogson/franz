use crate::atoms::{self, ok};
use crate::config::ConsumerConfig;
use crate::message::Message;
use futures::StreamExt;
use rdkafka::consumer::{
    BaseConsumer, CommitMode, Consumer, ConsumerContext, Rebalance, StreamConsumer,
};
use rdkafka::Message as _;
use rdkafka::{ClientConfig, ClientContext, TopicPartitionList};
use rustler::runtime::Channel;
use rustler::{
    Atom, Decoder, Encoder, Env, Error, LocalPid, NifTuple, OwnedEnv, Resource, ResourceArc, Term,
};
use std::panic::AssertUnwindSafe;
use std::time::Duration;
use tracing::trace;

#[derive(Clone, Copy, Debug)]
#[allow(clippy::enum_variant_names)]
pub enum Offset {
    Beginning,
    End,
    Stored,
    Invalid,
    Offset(i64),
    OffsetTail(i64),
}

impl From<&rdkafka::Offset> for Offset {
    fn from(offset: &rdkafka::Offset) -> Offset {
        match offset {
            rdkafka::Offset::Beginning => Offset::Beginning,
            rdkafka::Offset::End => Offset::End,
            rdkafka::Offset::Stored => Offset::Stored,
            rdkafka::Offset::Invalid => Offset::Invalid,
            rdkafka::Offset::Offset(n) => Offset::Offset(*n),
            rdkafka::Offset::OffsetTail(n) => Offset::OffsetTail(*n),
        }
    }
}

impl Encoder for Offset {
    fn encode<'b>(&self, env: Env<'b>) -> Term<'b> {
        match self {
            Offset::Beginning => atoms::beginning().encode(env),
            Offset::End => atoms::end().encode(env),
            Offset::Stored => atoms::stored().encode(env),
            Offset::Invalid => atoms::invalid().encode(env),
            Offset::Offset(n) => (atoms::offset(), n).encode(env),
            Offset::OffsetTail(n) => (atoms::offset_tail(), n).encode(env),
        }
    }
}

impl<'a> Decoder<'a> for Offset {
    fn decode(term: Term<'a>) -> Result<Self, Error> {
        // Try to decode as an atom first
        if let Ok(atom) = term.decode::<Atom>() {
            if atom == atoms::beginning() {
                return Ok(Offset::Beginning);
            } else if atom == atoms::end() {
                return Ok(Offset::End);
            } else if atom == atoms::stored() {
                return Ok(Offset::Stored);
            } else if atom == atoms::invalid() {
                return Ok(Offset::Invalid);
            }
        }

        // Try to decode as a tuple (atom, i64)
        if let Ok((atom, n)) = term.decode::<(Atom, i64)>() {
            if atom == atoms::offset() {
                return Ok(Offset::Offset(n));
            } else if atom == atoms::offset_tail() {
                return Ok(Offset::OffsetTail(n));
            }
        }

        Err(Error::BadArg)
    }
}

#[derive(NifTuple)]
struct TopicPartitionOffset(String, i32, i64);

// Commands sent from Elixir to the consumer
#[derive(Debug, rustler::NifTaggedEnum)]
enum ConsumerCommand {
    Subscribe {
        topics: Vec<String>,
    },
    Unsubscribe,
    Assignment,
    Commit {
        topic: String,
        partition: i32,
        offset: i64,
    },
    Committed {
        timeout: u64,
    },
    Pause {
        partitions: Vec<(String, i32)>,
    },
    Resume {
        partitions: Vec<(String, i32)>,
    },
    Seek {
        topic: String,
        partition: i32,
        offset: Offset,
    },
    Position {
        partitions: Vec<(String, i32)>,
    },
    Watermarks {
        topic: String,
        partition: i32,
        timeout: u64,
    },
}

// Events sent from consumer to Elixir
#[derive(Debug, rustler::NifTaggedEnum)]
enum ConsumerEvent {
    Message {
        msg: Message,
    },
    PreRebalance {
        action: RebalanceAction,
    },
    PostRebalance {
        action: RebalanceAction,
    },
    Assignments {
        assignments: Vec<(String, i32, Offset)>,
    },
    Committed {
        offsets: Vec<(String, i32, Offset)>,
    },
    Position {
        positions: Vec<(String, i32, Offset)>,
    },
    Watermarks {
        low: i64,
        high: i64,
    },
    Error {
        reason: String,
    },
    Ok,
}

#[derive(Clone, Debug, rustler::NifTaggedEnum)]
enum RebalanceAction {
    Assign {
        partitions: Vec<(String, i32, Offset)>,
    },
    Revoke {
        partitions: Vec<(String, i32)>,
    },
    Error {
        reason: String,
    },
}

struct Context {
    owner: LocalPid,
}

impl Context {
    fn handle_rebalance(&self, pre_or_post: Atom, rebalance: &Rebalance) {
        let mut env = OwnedEnv::new();
        let _ = env.send_and_clear(&self.owner, move |env| match rebalance {
            Rebalance::Assign(tpl) => {
                let assignments: Term = tpl
                    .elements()
                    .iter()
                    .map(|e| (e.topic(), e.partition(), Offset::from(&e.offset())))
                    .collect::<Vec<_>>()
                    .encode(env);
                (pre_or_post, (atoms::assign(), assignments)).encode(env)
            }
            Rebalance::Error(error) => {
                (pre_or_post, (atoms::error(), error.to_string())).encode(env)
            }
            Rebalance::Revoke(tpl) => {
                let partitions: Term = tpl
                    .elements()
                    .iter()
                    .map(|e| (e.topic(), e.partition()))
                    .collect::<Vec<_>>()
                    .encode(env);
                (pre_or_post, (atoms::revoke(), partitions)).encode(env)
            }
        });
    }
}

impl ClientContext for Context {}

impl ConsumerContext for Context {
    fn pre_rebalance(&self, _: &BaseConsumer<Self>, rebalance: &Rebalance) {
        self.handle_rebalance(atoms::pre_rebalance(), rebalance);
    }

    fn post_rebalance(&self, _: &BaseConsumer<Self>, rebalance: &Rebalance) {
        self.handle_rebalance(atoms::post_rebalance(), rebalance);
    }
}

// Rebalance event type for sending through mpsc channel
#[derive(Clone, Debug)]
enum RebalanceEvent {
    PreRebalance(RebalanceAction),
    PostRebalance(RebalanceAction),
}

// Context for consumer_stream task that sends rebalance events through mpsc channel
struct RebalanceContext {
    rebalance_tx: tokio::sync::mpsc::UnboundedSender<RebalanceEvent>,
}

impl RebalanceContext {
    fn handle_rebalance(
        &self,
        event_type: fn(RebalanceAction) -> RebalanceEvent,
        rebalance: &Rebalance,
    ) {
        let action = match rebalance {
            Rebalance::Assign(tpl) => {
                let partitions: Vec<_> = tpl
                    .elements()
                    .iter()
                    .map(|e| {
                        (
                            e.topic().to_string(),
                            e.partition(),
                            Offset::from(&e.offset()),
                        )
                    })
                    .collect();
                RebalanceAction::Assign { partitions }
            }
            Rebalance::Error(error) => RebalanceAction::Error {
                reason: error.to_string(),
            },
            Rebalance::Revoke(tpl) => {
                let partitions: Vec<_> = tpl
                    .elements()
                    .iter()
                    .map(|e| (e.topic().to_string(), e.partition()))
                    .collect();
                RebalanceAction::Revoke { partitions }
            }
        };
        let _ = self.rebalance_tx.send(event_type(action));
    }
}

impl ClientContext for RebalanceContext {}

impl ConsumerContext for RebalanceContext {
    fn pre_rebalance(&self, _: &BaseConsumer<Self>, rebalance: &Rebalance) {
        self.handle_rebalance(RebalanceEvent::PreRebalance, rebalance);
    }

    fn post_rebalance(&self, _: &BaseConsumer<Self>, rebalance: &Rebalance) {
        self.handle_rebalance(RebalanceEvent::PostRebalance, rebalance);
    }
}

struct ConsumerResource {
    _consumer: AssertUnwindSafe<StreamConsumer<Context>>,
}

#[rustler::resource_impl]
impl Resource for ConsumerResource {}

#[rustler::nif(name = "consumer_start")]
fn start(env: Env, config: ConsumerConfig) -> Result<ResourceArc<ConsumerResource>, String> {
    let cfg: ClientConfig = config.into();
    let consumer: StreamConsumer<Context> =
        cfg.create_with_context(Context { owner: env.pid() })
            .map_err(|e| format!("Failed to create Kafka consumer: {}", e))?;

    Ok(ResourceArc::new(ConsumerResource {
        _consumer: AssertUnwindSafe(consumer),
    }))
}

// Main consumer loop using Channel for bidirectional communication
// The Channel parameter is automatically created by the task macro, not passed from Elixir!
// Channel must be the FIRST parameter, then regular parameters
#[rustler::task]
async fn consumer_stream(
    channel: Channel<ConsumerCommand, ConsumerEvent>,
    config: ConsumerConfig,
) -> Result<(), String> {
    // Create an mpsc channel for rebalance events
    let (rebalance_tx, mut rebalance_rx) = tokio::sync::mpsc::unbounded_channel();

    // Create context with rebalance sender
    let context = RebalanceContext { rebalance_tx };

    // Create the consumer with context for rebalance callbacks
    let cfg: ClientConfig = config.into();
    let consumer: StreamConsumer<RebalanceContext> = cfg
        .create_with_context(context)
        .map_err(|e| format!("Failed to create Kafka consumer: {}", e))?;

    let mut channel = channel;
    let mut stream = consumer.stream();

    loop {
        tokio::select! {
            // Handle incoming messages from Kafka
            Some(msg_result) = stream.next() => {
                match msg_result {
                    Ok(msg) => {
                        trace!("Received message from Kafka: topic={}, partition={}, offset={}",
                               msg.topic(), msg.partition(), msg.offset());
                        channel.send(ConsumerEvent::Message {
                            msg: Message::from(&msg),
                        });
                    }
                    Err(err) => {
                        trace!("Error receiving message from Kafka: {:?}", err);
                        channel.send(ConsumerEvent::Error {
                            reason: err.to_string(),
                        });
                    }
                }
            }

            // Handle rebalance events
            Some(rebalance_event) = rebalance_rx.recv() => {
                trace!("Received rebalance event: {:?}", rebalance_event);
                match rebalance_event {
                    RebalanceEvent::PreRebalance(action) => {
                        channel.send(ConsumerEvent::PreRebalance { action });
                    }
                    RebalanceEvent::PostRebalance(action) => {
                        channel.send(ConsumerEvent::PostRebalance { action });
                    }
                }
            }

            // Handle commands from Elixir
            Some(cmd) = channel.next() => {
                trace!("Received command from Elixir: {:?}", cmd);
                match cmd {
                    ConsumerCommand::Subscribe { topics } => {
                        if topics.is_empty() {
                            channel.send(ConsumerEvent::Error {
                                reason: "Empty topics".to_string(),
                            });
                        } else {
                            let topics: Vec<_> = topics.iter().map(|s| s.as_str()).collect();
                            trace!("Subscribing to topics={:?}", &topics);

                            match consumer.subscribe(&topics) {
                                Ok(_) => channel.send(ConsumerEvent::Ok),
                                Err(e) => channel.send(ConsumerEvent::Error {
                                    reason: e.to_string(),
                                }),
                            }
                        }
                    }

                    ConsumerCommand::Unsubscribe => {
                        consumer.unsubscribe();
                        channel.send(ConsumerEvent::Ok);
                    }

                    ConsumerCommand::Assignment => {
                        trace!("Fetching assignments");
                        match consumer.assignment() {
                            Ok(tpl) => {
                                let assignments: Vec<_> = tpl
                                    .elements()
                                    .iter()
                                    .map(|e| (e.topic().to_string(), e.partition(), Offset::from(&e.offset())))
                                    .collect();
                                channel.send(ConsumerEvent::Assignments { assignments });
                            }
                            Err(err) => {
                                channel.send(ConsumerEvent::Error {
                                    reason: err.to_string(),
                                });
                            }
                        }
                    }

                    ConsumerCommand::Commit { topic, partition, offset } => {
                        let mut tpl = TopicPartitionList::new();
                        let _ = tpl.add_partition_offset(&topic, partition, rdkafka::Offset::Offset(offset));

                        trace!("Committing topic={}, partition={}, offset={}", &topic, &partition, &offset);

                        match consumer.commit(&tpl, CommitMode::Sync) {
                            Ok(()) => channel.send(ConsumerEvent::Ok),
                            Err(err) => {
                                channel.send(ConsumerEvent::Error {
                                    reason: err.to_string(),
                                });
                            }
                        }
                    }

                    ConsumerCommand::Committed { timeout } => {
                        trace!("Fetching committed");
                        match consumer.committed(Duration::from_millis(timeout)) {
                            Ok(tpl) => {
                                let offsets: Vec<_> = tpl
                                    .elements()
                                    .iter()
                                    .map(|e| (e.topic().to_string(), e.partition(), Offset::from(&e.offset())))
                                    .collect();
                                channel.send(ConsumerEvent::Committed { offsets });
                            }
                            Err(err) => {
                                channel.send(ConsumerEvent::Error {
                                    reason: err.to_string(),
                                });
                            }
                        }
                    }

                    ConsumerCommand::Pause { partitions } => {
                        let mut tpl = TopicPartitionList::new();
                        for (topic, partition) in partitions {
                            let _ = tpl.add_partition(&topic, partition);
                        }

                        trace!("Pausing partitions: {:?}", &tpl);

                        match consumer.pause(&tpl) {
                            Ok(()) => channel.send(ConsumerEvent::Ok),
                            Err(err) => {
                                channel.send(ConsumerEvent::Error {
                                    reason: err.to_string(),
                                });
                            }
                        }
                    }

                    ConsumerCommand::Resume { partitions } => {
                        let mut tpl = TopicPartitionList::new();
                        for (topic, partition) in partitions {
                            let _ = tpl.add_partition(&topic, partition);
                        }

                        trace!("Resuming partitions: {:?}", &tpl);

                        match consumer.resume(&tpl) {
                            Ok(()) => channel.send(ConsumerEvent::Ok),
                            Err(err) => {
                                channel.send(ConsumerEvent::Error {
                                    reason: err.to_string(),
                                });
                            }
                        }
                    }

                    ConsumerCommand::Seek { topic, partition, offset } => {
                        let rdkafka_offset = match offset {
                            Offset::Beginning => rdkafka::Offset::Beginning,
                            Offset::End => rdkafka::Offset::End,
                            Offset::Stored => rdkafka::Offset::Stored,
                            Offset::Invalid => rdkafka::Offset::Invalid,
                            Offset::Offset(n) => rdkafka::Offset::Offset(n),
                            Offset::OffsetTail(n) => rdkafka::Offset::OffsetTail(n),
                        };

                        trace!("Seeking topic={}, partition={}, offset={:?}", &topic, &partition, &rdkafka_offset);

                        match consumer.seek(&topic, partition, rdkafka_offset, Duration::from_secs(5)) {
                            Ok(()) => channel.send(ConsumerEvent::Ok),
                            Err(err) => {
                                channel.send(ConsumerEvent::Error {
                                    reason: err.to_string(),
                                });
                            }
                        }
                    }

                    ConsumerCommand::Position { partitions } => {
                        let mut tpl = TopicPartitionList::new();
                        for (topic, partition) in partitions {
                            let _ = tpl.add_partition(&topic, partition);
                        }

                        trace!("Fetching position for partitions");

                        match consumer.position() {
                            Ok(position_tpl) => {
                                let positions: Vec<_> = position_tpl
                                    .elements()
                                    .iter()
                                    .map(|e| (e.topic().to_string(), e.partition(), Offset::from(&e.offset())))
                                    .collect();
                                channel.send(ConsumerEvent::Position { positions });
                            }
                            Err(err) => {
                                channel.send(ConsumerEvent::Error {
                                    reason: err.to_string(),
                                });
                            }
                        }
                    }

                    ConsumerCommand::Watermarks { topic, partition, timeout } => {
                        trace!("Fetching watermarks for topic={}, partition={}", &topic, &partition);

                        let timeout_duration = Duration::from_millis(timeout);

                        match consumer.fetch_watermarks(&topic, partition, timeout_duration) {
                            Ok((low, high)) => {
                                trace!("Watermarks: low={}, high={}", low, high);
                                channel.send(ConsumerEvent::Watermarks { low, high });
                            }
                            Err(err) => {
                                channel.send(ConsumerEvent::Error {
                                    reason: err.to_string(),
                                });
                            }
                        }
                    }
                }
            }

            else => {
                // Both streams ended, exit loop
                break;
            }
        }
    }

    // Final message when task completes
    Ok(())
}

#[rustler::nif(name = "consumer_subscribe")]
fn subscribe(
    env: Env,
    sender: rustler::runtime::ChannelSender<ConsumerCommand>,
    topics: Vec<String>,
) -> rustler::NifResult<Atom> {
    rustler::runtime::channel::send(
        env,
        sender,
        ConsumerCommand::Subscribe { topics }.encode(env),
    )
}

#[rustler::nif(name = "consumer_unsubscribe")]
fn unsubscribe(
    env: Env,
    sender: rustler::runtime::ChannelSender<ConsumerCommand>,
) -> rustler::NifResult<Atom> {
    rustler::runtime::channel::send(env, sender, ConsumerCommand::Unsubscribe.encode(env))
}

#[rustler::nif(name = "consumer_assignment")]
fn assignment(
    env: Env,
    sender: rustler::runtime::ChannelSender<ConsumerCommand>,
) -> rustler::NifResult<Atom> {
    rustler::runtime::channel::send(env, sender, ConsumerCommand::Assignment.encode(env))
}

#[rustler::nif(name = "consumer_commit")]
fn commit(
    env: Env,
    sender: rustler::runtime::ChannelSender<ConsumerCommand>,
    tpo: TopicPartitionOffset,
) -> rustler::NifResult<Atom> {
    let TopicPartitionOffset(topic, partition, offset) = tpo;
    rustler::runtime::channel::send(
        env,
        sender,
        ConsumerCommand::Commit {
            topic,
            partition,
            offset,
        }
        .encode(env),
    )
}

#[rustler::nif(name = "consumer_committed")]
fn committed(
    env: Env,
    sender: rustler::runtime::ChannelSender<ConsumerCommand>,
    timeout: u64,
) -> rustler::NifResult<Atom> {
    rustler::runtime::channel::send(
        env,
        sender,
        ConsumerCommand::Committed { timeout }.encode(env),
    )
}

#[rustler::nif(name = "consumer_stop")]
fn stop(_resource: ResourceArc<ConsumerResource>) -> Atom {
    // The consumer will be dropped when the resource is garbage collected
    ok()
}

#[rustler::nif(name = "consumer_pause")]
fn pause(
    env: Env,
    sender: rustler::runtime::ChannelSender<ConsumerCommand>,
    partitions: Vec<(String, i32)>,
) -> rustler::NifResult<Atom> {
    rustler::runtime::channel::send(
        env,
        sender,
        ConsumerCommand::Pause { partitions }.encode(env),
    )
}

#[rustler::nif(name = "consumer_resume")]
fn resume(
    env: Env,
    sender: rustler::runtime::ChannelSender<ConsumerCommand>,
    partitions: Vec<(String, i32)>,
) -> rustler::NifResult<Atom> {
    rustler::runtime::channel::send(
        env,
        sender,
        ConsumerCommand::Resume { partitions }.encode(env),
    )
}

#[rustler::nif(name = "consumer_seek")]
fn seek(
    env: Env,
    sender: rustler::runtime::ChannelSender<ConsumerCommand>,
    topic: String,
    partition: i32,
    offset: Offset,
) -> rustler::NifResult<Atom> {
    rustler::runtime::channel::send(
        env,
        sender,
        ConsumerCommand::Seek {
            topic,
            partition,
            offset,
        }
        .encode(env),
    )
}

#[rustler::nif(name = "consumer_position")]
fn position(
    env: Env,
    sender: rustler::runtime::ChannelSender<ConsumerCommand>,
    partitions: Vec<(String, i32)>,
) -> rustler::NifResult<Atom> {
    rustler::runtime::channel::send(
        env,
        sender,
        ConsumerCommand::Position { partitions }.encode(env),
    )
}

#[rustler::nif(name = "consumer_watermarks")]
fn watermarks(
    env: Env,
    sender: rustler::runtime::ChannelSender<ConsumerCommand>,
    topic: String,
    partition: i32,
    timeout: u64,
) -> rustler::NifResult<Atom> {
    rustler::runtime::channel::send(
        env,
        sender,
        ConsumerCommand::Watermarks {
            topic,
            partition,
            timeout,
        }
        .encode(env),
    )
}
