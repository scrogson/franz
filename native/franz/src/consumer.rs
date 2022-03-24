use crate::atoms::{self, ok, error};
use crate::config::ConsumerConfig;
use crate::message::Message;
use crate::task;
use futures::StreamExt;
use rdkafka::{ClientContext, ClientConfig, TopicPartitionList};
use rdkafka::consumer::{ConsumerContext, StreamConsumer, CommitMode, Consumer, Rebalance};
use rustler::{Atom, Encoder, Env, NifTuple, OwnedEnv, LocalPid, ResourceArc, Term};
use std::sync::Mutex;
use std::time::Duration;
use tokio::sync::mpsc::{channel, Receiver, Sender};
use log::trace;

pub enum Offset {
    Beginning,
    End,
    Stored,
    Invalid,
    Offset(i64),
}

impl From<&rdkafka::Offset> for Offset {
    fn from(offset: &rdkafka::Offset) -> Offset {
        match offset {
            rdkafka::Offset::Beginning => Offset::Beginning,
            rdkafka::Offset::End => Offset::End,
            rdkafka::Offset::Stored => Offset::Stored,
            rdkafka::Offset::Invalid => Offset::Invalid,
            rdkafka::Offset::Offset(n) => Offset::Offset(*n),
        }
    }
}

impl<'a> Encoder for Offset {
    fn encode<'b>(&self, env: Env<'b>) -> Term<'b> {
        match self {
            Offset::Beginning => atoms::beginning().encode(env),
            Offset::End => atoms::end().encode(env),
            Offset::Stored => atoms::stored().encode(env),
            Offset::Invalid => atoms::invalid().encode(env),
            Offset::Offset(n) => (atoms::offset(), n).encode(env),
        }
    }
}

#[derive(NifTuple)]
struct TopicPartitonOffset(String, i32, i64);

enum Msg {
    Assignment(LocalPid),
    Commit(LocalPid, TopicPartitonOffset),
    Committed(LocalPid, u64),
    Poll(LocalPid),
    Stop,
    Subscribe(LocalPid, Vec<String>),
    Unsubscribe(LocalPid),
}

struct Ref(Mutex<Sender<Msg>>);

impl Ref {
    fn new(tx: Sender<Msg>) -> ResourceArc<Ref> {
        ResourceArc::new(Ref(Mutex::new(tx)))
    }
}

struct Context {
    owner: LocalPid,
}

impl Context {
    fn handle_rebalance(&self, pre_or_post: Atom, rebalance: &Rebalance) {
        let mut env = OwnedEnv::new();
        env.send_and_clear(&self.owner, move |env| {
            match rebalance {
                Rebalance::Assign(tpl) => {
                    let assignments: Term = tpl.elements().iter().map(|e| {
                        (e.topic(), e.partition(), Offset::from(&e.offset()))
                    }).collect::<Vec<_>>().encode(env);
                    (pre_or_post, (atoms::assign(), assignments)).encode(env)
                }
                Rebalance::Error(error) => {
                    (pre_or_post, (atoms::error(), error)).encode(env)
                }
                Rebalance::Revoke => {
                    (pre_or_post, atoms::revoke()).encode(env)
                }
            }
        });
    }
}

impl ClientContext for Context {}

impl ConsumerContext for Context {
    fn pre_rebalance(&self, rebalance: &Rebalance) {
        self.handle_rebalance(atoms::pre_rebalance(), rebalance);
    }

    fn post_rebalance(&self, rebalance: &Rebalance) {
        self.handle_rebalance(atoms::post_rebalance(), rebalance);
    }

    //fn commit_callback(
        //&self,
        //_result: rdkafka::error::KafkaResult<()>,
        //offsets: &TopicPartitionList,
    //) {
        //info!("Committing offsets: {:?}", offsets);
    //}

    fn message_queue_nonempty_callback(&self) {
        let mut env = OwnedEnv::new();
        env.send_and_clear(&self.owner, move |env| {
            atoms::poll_ready().encode(env)
        });
    }
}

pub fn load(env: Env) -> bool {
    rustler::resource!(Ref, env);
    true
}

#[rustler::nif(name = "consumer_start")]
fn start(env: Env, config: ConsumerConfig) -> (Atom, ResourceArc<Ref>) {
    let (tx, rx) = channel::<Msg>(1000);
    spawn_consumer(env.pid(), config, rx);

    (ok(), Ref::new(tx))
}

#[rustler::nif(name = "consumer_stop")]
fn stop(resource: ResourceArc<Ref>) -> Atom {
    send(resource, Msg::Stop);

    ok()
}

#[rustler::nif(name = "consumer_subscribe")]
fn subscribe(
    env: Env,
    resource: ResourceArc<Ref>,
    topics: Vec<String>
) -> (Atom, ResourceArc<Ref>) {
    send(resource.clone(), Msg::Subscribe(env.pid(), topics));

    (ok(), resource)
}

#[rustler::nif(name = "consumer_unsubscribe")]
fn unsubscribe(env: Env, resource: ResourceArc<Ref>) -> (Atom, ResourceArc<Ref>) {
    send(resource.clone(), Msg::Unsubscribe(env.pid()));

    (ok(), resource)
}

#[rustler::nif(name = "consumer_assignment")]
fn assignment(env: Env, resource: ResourceArc<Ref>) -> (Atom, ResourceArc<Ref>) {
    send(resource.clone(), Msg::Assignment(env.pid()));

    (ok(), resource)
}

#[rustler::nif(name = "consumer_poll")]
fn poll(env: Env, resource: ResourceArc<Ref>) -> (Atom, ResourceArc<Ref>) {
    send(resource.clone(), Msg::Poll(env.pid()));

    (ok(), resource)
}

#[rustler::nif(name = "consumer_commit")]
fn commit(env: Env, resource: ResourceArc<Ref>, tpo: TopicPartitonOffset) -> (Atom, ResourceArc<Ref>) {
    send(resource.clone(), Msg::Commit(env.pid(), tpo));

    (ok(), resource)
}

#[rustler::nif(name = "consumer_committed")]
fn committed(env: Env, resource: ResourceArc<Ref>, timeout: u64) -> (Atom, ResourceArc<Ref>) {
    send(resource.clone(), Msg::Committed(env.pid(), timeout));

    (ok(), resource)
}

fn send(resource: ResourceArc<Ref>, msg: Msg) {
    let lock = resource.0.lock().expect("Failed to obtain a lock");
    let mut sender = lock.clone();

    task::spawn(async move {
        match sender.send(msg).await {
            Ok(_) => (),
            Err(_err) => trace!("send error"),
        }
    });
}

fn spawn_consumer(owner: LocalPid, config: ConsumerConfig, mut rx: Receiver<Msg>) {
    task::spawn(async move {
        let cfg: ClientConfig = config.into();
        let consumer: StreamConsumer<Context> = cfg
            .create_with_context(Context { owner: owner.clone() })
            .expect("Failed to create Kafka consumer");
        let mut stream = consumer.start_with(Duration::from_millis(100), true);
        let mut env = OwnedEnv::new();

        loop {
            match rx.recv().await {
                Some(Msg::Assignment(pid)) => {
                    trace!("Fetching assignments");
                    match consumer.subscription() {
                        Ok(tpl) => {
                            trace!("{:?}", &tpl);
                            env.send_and_clear(&pid, move |env| {
                            let assignments: Term = tpl.elements().iter().map(|e| {
                                (e.topic(), e.partition(), Offset::from(&e.offset()))
                            }).collect::<Vec<_>>().encode(env);
                            (atoms::assignments(), assignments).encode(env)
                        })
                        },
                        Err(err) => env.send_and_clear(&pid, move |env| {
                            (error(), err.to_string()).encode(env)
                        })
                    }
                }

                Some(Msg::Commit(pid, TopicPartitonOffset(topic, partition, offset))) => {
                    let mut tpl = TopicPartitionList::new();
                    tpl.add_partition_offset(&topic, partition, rdkafka::Offset::Offset(offset));

                    trace!("Committing topic={}, partition={}, offset={}", &topic, &partition, &offset);

                    match consumer.commit(&tpl, CommitMode::Sync) {
                        Ok(()) => env.send_and_clear(&pid, move |env| {
                            (ok(), atoms::committed()).encode(env)
                        }),
                        Err(err) => env.send_and_clear(&pid, move |env| {
                            (error(), err.to_string()).encode(env)
                        })
                    }
                }

                Some(Msg::Committed(pid, timeout)) => {
                    trace!("Fetching committed");
                    match consumer.committed(Duration::from_millis(timeout)) {
                        Ok(tpl) => {
                            env.send_and_clear(&pid, move |env| {
                            let assignments: Term = tpl.elements().iter().map(|e| {
                                (e.topic(), e.partition(), Offset::from(&e.offset()))
                            }).collect::<Vec<_>>().encode(env);
                            (ok(), assignments).encode(env)
                        })
                        },
                        Err(err) => env.send_and_clear(&pid, move |env| {
                            (error(), err.to_string()).encode(env)
                        })
                    }
                }

                Some(Msg::Subscribe(pid, topics)) => {
                    if topics.is_empty() {
                        env.send_and_clear(&pid, move |env| (error(), "Empty topics").encode(env))
                    } else {
                        let topics: Vec<_> = topics.iter().map(|s| s.as_str()).collect();
                        trace!("Subscribing to topics={:?}", &topics);
                        consumer.subscribe(&topics).expect(&format!("Failed to subscribe to topics: {:?}", &topics));
                        env.send_and_clear(&pid, move |env| ok().encode(env))
                    }
                }

                Some(Msg::Unsubscribe(pid)) => {
                    consumer.unsubscribe();
                    env.send_and_clear(&pid, move |env| ok().encode(env))
                }

                Some(Msg::Poll(pid)) => {
                    match stream.next().await {
                        Some(Ok(msg)) => {
                            env.send_and_clear(&pid, move |env| {
                                Message::from(&msg).encode(env)
                            });
                        }

                        Some(Err(err)) => {
                            env.send_and_clear(&pid, move |env| {
                                (error(), err.to_string()).encode(env)
                            });
                        }

                        None => {},
                    }
                }

                Some(Msg::Stop) => break,

                None => {}
            }
        }
    });
}
