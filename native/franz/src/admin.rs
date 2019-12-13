use crate::atoms::{error, ok};
use crate::config::AdminConfig;
use crate::task;
use log::trace;
use rdkafka::client::DefaultClientContext;
use rdkafka::config::{ClientConfig, RDKafkaLogLevel};
use rdkafka::admin::{AdminClient, };
use rdkafka::message::{BorrowedMessage, Message as _};
use rustler::{
    Atom, Decoder, Encoder, Env, Error, NifStruct, OwnedBinary, OwnedEnv, Pid, ResourceArc, Term,
};
use std::sync::Mutex;
use tokio::sync::mpsc::{channel, Receiver, Sender};

#[derive(NifStruct)]
#[module = "Franz.NewTopic"]
/// Configuration for a CreateTopic operation.
pub struct NewTopic {
    name: String,
    num_partitions: i32,
    // TODO: Support TopicReplication here.
    replication: i32,
    config: Vec<(String, String)>,
}

pub struct AdminRef(Mutex<Sender<AdminMsg>>);

impl AdminRef {
    fn new(tx: Sender<AdminMsg>) -> ResourceArc<AdminRef> {
        ResourceArc::new(AdminRef(Mutex::new(tx)))
    }
}

enum AdminMsg {
    CreateTopics(Pid, Vec<NewTopic>),
    Stop,
}

pub fn load(env: Env) -> bool {
    rustler::resource!(AdminRef, env);
    true
}


#[rustler::nif(name = "admin_start", schedule = "DirtyIo")]
fn start(env: Env, config: AdminConfig) -> (Atom, ResourceArc<AdminRef>) {
    let pid = env.pid();
    let (tx, rx) = channel::<AdminMsg>(1000);

    spawn_client(pid, config, rx);

    (ok(), AdminRef::new(tx))
}

#[rustler::nif(name = "admin_stop", schedule = "DirtyIo")]
fn stop(resource: ResourceArc<AdminRef>) -> Atom {
    let lock = resource.0.lock().expect("Failed to obtain a lock");
    let mut sender = lock.clone();

    task::spawn(async move {
        match sender.send(AdminMsg::Stop).await {
            Ok(_) => (),
            Err(_err) => trace!("send error"),
        }
    });

    ok()
}


#[rustler::nif(schedule = "DirtyIo")]
pub fn create_topics(env: Env, resource: ResourceArc<AdminRef>, topics: Vec<NewTopic>) -> (Atom, ResourceArc<AdminRef>) {
    let pid = env.pid();
    let lock = resource.0.lock().expect("Failed to obtain a lock");
    let mut sender = lock.clone();

    task::spawn(async move {
        match sender.send(AdminMsg::CreateTopics(pid, topics)).await {
            Ok(_) => (),
            Err(_err) => trace!("send error"),
        }
    });

    (ok(), resource.clone())
}

fn spawn_client(owner: Pid, config: AdminConfig, mut rx: Receiver<AdminMsg>) {
    task::spawn(async move {
        use AdminMsg::*;

        let mut env = OwnedEnv::new();
        let mut cfg = ClientConfig::new();

        cfg.set("bootstrap.servers", &config.bootstrap_servers);
        cfg.set_log_level(RDKafkaLogLevel::Debug);

        let admin: AdminClient<DefaultClientContext> = cfg.create().expect("Failed to create Kafka client");
        let admin_options = rdkafka::admin::AdminOptions::new();

        loop {
            match rx.recv().await {
                Some(CreateTopics(pid, new_topics)) => {
                    let new_topics = new_topics;
                    let topics: Vec<rdkafka::admin::NewTopic> = new_topics.iter().map(|new_topic| {
                        let mut topic = rdkafka::admin::NewTopic::new(
                            &new_topic.name,
                            new_topic.num_partitions,
                            rdkafka::admin::TopicReplication::Fixed(new_topic.replication),
                        );

                        for (k, v) in &new_topic.config {
                            topic = topic.set(&k, &v);
                        }

                        topic
                    }).collect();

                    match &admin.create_topics(&topics, &admin_options).await {
                        Ok(results) => {
                            trace!("{:?}", results);
                            let mut topic_results: Vec<Result<String, (String, String)>> = Vec::new();
                            for result in results {
                                match result {
                                    Ok(topic) => topic_results.push(Ok(topic.to_string())),
                                    Err((topic, error)) => topic_results.push(Err((topic.to_string(), error.to_string()))),
                                }
                            }

                            env.send_and_clear(&pid, move |env| topic_results.encode(env))
                        }
                        Err(err) => {
                            env.send_and_clear(&pid, move |env| err.to_string().encode(env))
                        }
                    }
                }
                Some(Stop) => break,
                None => continue,
            }
        }
    });
}
