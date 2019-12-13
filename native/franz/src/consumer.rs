//use crate::TOKIO;
use crate::atoms::{ok, error};
use crate::config::ConsumerConfig;
use crate::message::Message;
use crate::task;
use rdkafka::consumer::{BaseConsumer, Consumer};
use rdkafka::config::{ClientConfig, RDKafkaLogLevel};
use rustler::{Atom, Encoder, Env, Error, OwnedEnv, Pid, ResourceArc, Term};
use std::sync::Mutex;
use std::time::Duration;
use tokio::sync::mpsc::{channel, Receiver, Sender};
use log::trace;

struct ConsumerRef(Mutex<Sender<ConsumerMsg>>);

impl ConsumerRef {
    fn new(tx: Sender<ConsumerMsg>) -> ResourceArc<ConsumerRef> {
        ResourceArc::new(ConsumerRef(Mutex::new(tx)))
    }
}

enum ConsumerMsg {
    Poll(Pid, u64),
    Stop,
}

pub fn load(env: Env) -> bool {
    rustler::resource!(ConsumerRef, env);
    true
}

#[rustler::nif(name = "consumer_start", schedule = "DirtyIo")]
fn start(env: Env, config: ConsumerConfig) -> Result<(Atom, ResourceArc<ConsumerRef>), Error> {
    let pid = env.pid();
    let (tx, rx) = channel::<ConsumerMsg>(1000);

    spawn_consumer(pid, config, rx);

    Ok((ok(), ConsumerRef::new(tx)))
}

#[rustler::nif(name = "consumer_stop", schedule = "DirtyIo")]
fn stop(resource: ResourceArc<ConsumerRef>) -> Atom {
    let lock = resource.0.lock().expect("Failed to obtain a lock");
    let mut sender = lock.clone();

    task::spawn(async move {
        match sender.send(ConsumerMsg::Stop).await {
            Ok(_) => (),
            Err(_err) => trace!("send error"),
        }
    });

    ok()
}

#[rustler::nif(name = "consumer_poll", schedule = "DirtyIo")]
fn poll(env: Env, resource: ResourceArc<ConsumerRef>, timeout: u64) -> (Atom, ResourceArc<ConsumerRef>) {
    let pid = env.pid();
    let lock = resource.0.lock().expect("Failed to obtain a lock");
    let mut sender = lock.clone();

    task::spawn(async move {
        match sender.send(ConsumerMsg::Poll(pid, timeout)).await {
            Ok(_) => (),
            Err(_err) => trace!("send error"),
        }
    });

    (ok(), resource.clone())
}

fn spawn_consumer(owner: Pid, config: ConsumerConfig, mut rx: Receiver<ConsumerMsg>) {
    task::spawn(async move {
        use ConsumerMsg::*;

        let mut env = OwnedEnv::new();
        let mut cfg = ClientConfig::new();

        cfg.set("auto.offset.reset", &config.auto_offset_reset.to_string());
        cfg.set("bootstrap.servers", &config.bootstrap_servers);
        cfg.set("enable.auto.commit", &config.enable_auto_commit.to_string());

        if let Some(group_id) = &config.group_id {
            cfg.set("group.id", group_id);
        }

        cfg.set_log_level(RDKafkaLogLevel::Debug);

        let consumer: BaseConsumer = cfg.create().expect("Failed to create Kafka consumer");

        if config.topics.is_empty() {
            env.send_and_clear(&owner, move |env| (error(), "Empty topics").encode(env));
            return
        } else {
            let topics: Vec<_> = config.topics.iter().map(|s| s.as_str()).collect();
            consumer.subscribe(&topics).expect(&format!("Failed to subscribe to topics: {:?}", &topics));
        }

        loop {
            match rx.recv().await {
                Some(Poll(pid, timeout)) => {
                    match &consumer.poll(Duration::from_millis(timeout)) {
                        Some(Ok(msg)) => {
                            env.send_and_clear(&pid, move |env| {
                                Message::from(msg).encode(env)
                            });
                        }
                        Some(Err(err)) => {
                            env.send_and_clear(&pid, move |env| {
                                (error(), format!("{:?}", err)).encode(env)
                            });
                        }
                        None => {
                            env.send_and_clear(&pid, move |env| {
                                None::<Term>.encode(env)
                            });
                        }
                    }
                }
                Some(Stop) => break,
                None => continue,
            }
        }
    });
}
