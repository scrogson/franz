use crate::TOKIO;
use crate::atoms::{ok, error};
use rdkafka::consumer::{BaseConsumer, Consumer};
use rdkafka::config::{ClientConfig, RDKafkaLogLevel};
use rdkafka::message::{Message as _, OwnedMessage};
use rustler::{Atom, Decoder, Encoder, Env, Error, NifStruct, OwnedBinary, OwnedEnv, Pid, ResourceArc, Term};
use std::sync::Mutex;
use std::time::Duration;
use tokio::sync::mpsc::{channel, Receiver, Sender};
use std::io::Write;

//pub struct Bin(Vec<u8>);

//impl<'a> Encoder for Bin {
    //fn encode<'b>(&self, env: Env<'b>) -> Term<'b> {
        //let mut bin = OwnedBinary::new(self.0.len()).expect("Failed to alloc");
        //bin.as_mut_slice().write_all(&self.0).unwrap();
        //bin.release(env).encode(env)
    //}
//}

//impl<'a> Decoder<'a> for Bin {
    //fn decode(term: Term<'a>) -> Result<Bin, Error> {
        //let vec: Vec<u8> = term.decode()?;
        //Ok(Bin(vec))
    //}
//}

#[derive(NifStruct)]
#[module = "Franz.Message"]
pub struct Message {
    payload: Option<Vec<u8>>,
    key: Option<Vec<u8>>,
    topic: String,
    //timestamp: Option<i64>,
    //partition: i32,
    //offset: i64,
    //headers: Vec<(String, String)>,
}

impl From<OwnedMessage> for Message {
    fn from(o: OwnedMessage) -> Message {
        let payload = o.payload().map(|p| p.to_vec());
        eprintln!("Payload: {:?}", payload);
        let key = o.key().map(|k| k.to_vec());
        eprintln!("Key: {:?}", key);
        let topic = o.topic().to_owned();
        eprintln!("Topic: {:?}", topic);

        Message {
            payload,
            key,
            topic,
            //key: o.key().map(|k| k.to_vec()),
            //topic: o.topic().to_owned(),
            //timestamp: o.timestamp().to_millis(),
            //partition: o.partition(),
            //offset: o.offset(),
            ////headers: self.headers().map(),
            //headers: vec![],
        }
    }
}

#[derive(NifStruct)]
#[module = "Franz.Config"]
pub struct Config {
    auto_offset_reset: String,
    brokers: String,
    enable_auto_commit: bool,
    group_id: Option<String>,
    topics: Vec<String>,
}

struct ClientRef(Mutex<Sender<ClientMsg>>);

impl ClientRef {
    fn new(tx: Sender<ClientMsg>) -> ResourceArc<ClientRef> {
        ResourceArc::new(ClientRef(Mutex::new(tx)))
    }
}

enum ClientMsg {
    Poll(Pid, u64),
    Stop,
}

pub fn load(env: Env) -> bool {
    rustler::resource!(ClientRef, env);
    true
}

#[rustler::nif(name = "client_start", schedule = "DirtyIo")]
fn start(env: Env, config: Config) -> Result<(Atom, ResourceArc<ClientRef>), Error> {
    let pid = env.pid();
    let (tx, rx) = channel::<ClientMsg>(1000);

    spawn_client(pid, config, rx);

    Ok((ok(), ClientRef::new(tx)))
}

#[rustler::nif(name = "client_poll", schedule = "DirtyIo")]
fn poll(env: Env, resource: ResourceArc<ClientRef>, timeout: u64) -> Term {
    let pid = env.pid();
    let lock = resource.0.lock().expect("Failed to obtain a lock");
    let mut sender = lock.clone();

    TOKIO.spawn(async move {
        match sender.send(ClientMsg::Poll(pid, timeout)).await {
            Ok(_) => (),
            Err(_err) => println!("send error"),
        }
    });

    (ok(), resource.clone()).encode(env)
}

fn spawn_client(owner: Pid, config: Config, mut rx: Receiver<ClientMsg>) {
    TOKIO.spawn(async move {
        use ClientMsg::*;

        let mut env = OwnedEnv::new();
        let mut cfg = ClientConfig::new();

        cfg.set("auto.offset.reset", &config.auto_offset_reset);
        cfg.set("bootstrap.servers", &config.brokers);
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
                                Message::from(msg.detach()).encode(env)
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
