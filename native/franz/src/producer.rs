use crate::atoms::{ok, error};
use crate::config::ProducerConfig;
use crate::message::Message;
use crate::task;
use rdkafka::producer::{FutureProducer, FutureRecord};
use rdkafka::config::{ClientConfig, RDKafkaLogLevel};
use rustler::{Atom, Encoder, Env, OwnedEnv, Pid, ResourceArc};
use std::sync::Mutex;
use tokio::sync::mpsc::{channel, Receiver, Sender};
use log::{error, trace};

struct Ref(Mutex<Sender<Msg>>);

impl Ref {
    fn new(tx: Sender<Msg>) -> ResourceArc<Ref> {
        ResourceArc::new(Ref(Mutex::new(tx)))
    }
}

enum Msg {
    Send(Pid, Message),
    Stop,
}

pub fn load(env: Env) -> bool {
    rustler::resource!(Ref, env);
    true
}

#[rustler::nif(name = "producer_start")]
fn start(config: ProducerConfig) -> (Atom, ResourceArc<Ref>) {
    let (tx, rx) = channel::<Msg>(1000);
    spawn_producer(config, rx);
    (ok(), Ref::new(tx))
}

#[rustler::nif(name = "producer_stop")]
fn stop(resource: ResourceArc<Ref>) -> Atom {
    send(resource, Msg::Stop);
    ok()
}

#[rustler::nif(name = "producer_send")]
fn deliver(env: Env, resource: ResourceArc<Ref>, msg: Message) -> (Atom, ResourceArc<Ref>) {
    send(resource.clone(), Msg::Send(env.pid(), msg));
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

fn spawn_producer(config: ProducerConfig, mut rx: Receiver<Msg>) {
    task::spawn(async move {
        let mut env = OwnedEnv::new();
        let mut cfg = ClientConfig::new();

        cfg.set("bootstrap.servers", &config.bootstrap_servers);
        cfg.set_log_level(RDKafkaLogLevel::Debug);

        let producer: FutureProducer = cfg.create().expect("Failed to create Kafka producer");

        loop {
            match rx.recv().await {
                Some(Msg::Send(pid, msg)) => {
                    let mut record = FutureRecord::to(&msg.topic);

                    if let Some(payload) = &msg.payload {
                        record = record.payload(&payload.0);
                    }
                    if let Some(key) = &msg.key {
                        record = record.key(&key.0);
                    }

                    record = record.partition(msg.partition);

                    if let Some(timestamp) = msg.timestamp {
                        record = record.timestamp(timestamp);
                    }

                    match &producer.send(record, 0).await {
                        Ok(msg) => {
                            trace!("{:?}", msg);
                            env.send_and_clear(&pid, move |env| {
                                ok().encode(env)
                            });
                        }
                        Err(err) => {
                            error!("{:?}", err);
                            env.send_and_clear(&pid, move |env| {
                                (error(), format!("{:?}", err)).encode(env)
                            });
                        }
                    }
                }
                Some(Msg::Stop) => break,
                None => continue,
            }
        }
    });
}
