use crate::TOKIO;
use crate::atoms::{ok, error};
use crate::config::ProducerConfig;
use crate::message::Message;
use rdkafka::producer::{FutureProducer, Producer};
use rdkafka::config::{ClientConfig, RDKafkaLogLevel};
use rustler::{Atom, Decoder, Encoder, Env, Error, NifStruct, OwnedBinary, OwnedEnv, Pid, ResourceArc, Term};
use std::sync::Mutex;
use std::time::Duration;
use tokio::sync::mpsc::{channel, Receiver, Sender};
use log::trace;

struct ProducerRef(Mutex<Sender<ProducerMsg>>);

impl ProducerRef {
    fn new(tx: Sender<ProducerMsg>) -> ResourceArc<ProducerRef> {
        ResourceArc::new(ProducerRef(Mutex::new(tx)))
    }
}

enum ProducerMsg {
    Poll(Pid, u64),
    Stop,
}

pub fn load(env: Env) -> bool {
    rustler::resource!(ProducerRef, env);
    true
}

#[rustler::nif(name = "producer_start", schedule = "DirtyIo")]
fn start(env: Env, config: ProducerConfig) -> (Atom, ResourceArc<ProducerRef>) {
    let pid = env.pid();
    let (tx, rx) = channel::<ProducerMsg>(1000);

    spawn_producer(pid, config, rx);

    (ok(), ProducerRef::new(tx))
}

#[rustler::nif(name = "producer_stop", schedule = "DirtyIo")]
fn stop(resource: ResourceArc<ProducerRef>, timeout: u64) -> Atom {
    let lock = resource.0.lock().expect("Failed to obtain a lock");
    let mut sender = lock.clone();

    TOKIO.spawn(async move {
        match sender.send(ProducerMsg::Stop).await {
            Ok(_) => (),
            Err(_err) => trace!("send error"),
        }
    });

    ok()
}

#[rustler::nif(name = "producer_poll", schedule = "DirtyIo")]
fn poll(env: Env, resource: ResourceArc<ProducerRef>, timeout: u64) -> Term {
    let pid = env.pid();
    let lock = resource.0.lock().expect("Failed to obtain a lock");
    let mut sender = lock.clone();

    TOKIO.spawn(async move {
        match sender.send(ProducerMsg::Poll(pid, timeout)).await {
            Ok(_) => (),
            Err(_err) => trace!("send error"),
        }
    });

    (ok(), resource.clone()).encode(env)
}

fn spawn_producer(owner: Pid, config: ProducerConfig, mut rx: Receiver<ProducerMsg>) {
    TOKIO.spawn(async move {
        let mut env = OwnedEnv::new();
        let mut cfg = ClientConfig::new();

        cfg.set("bootstrap.servers", &config.bootstrap_servers);
        cfg.set_log_level(RDKafkaLogLevel::Debug);

        let producer: FutureProducer = cfg.create().expect("Failed to create Kafka producer");

        loop {
            match rx.recv().await {
                Some(ProducerMsg::Send(pid, timeout)) => {
                    match &producer.poll(Duration::from_millis(timeout)) {
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
