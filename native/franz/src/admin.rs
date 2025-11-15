use crate::atoms::ok;
use crate::config::AdminConfig;
use rdkafka::client::DefaultClientContext;
use rdkafka::config::{ClientConfig, RDKafkaLogLevel};
use rdkafka::admin::AdminClient;
use rustler::{Atom, NifStruct, ResourceArc};
use std::panic::AssertUnwindSafe;

#[derive(NifStruct, Clone)]
#[module = "Franz.NewTopic"]
/// Configuration for a CreateTopic operation.
pub struct NewTopic {
    name: String,
    num_partitions: i32,
    // TODO: Support TopicReplication here.
    replication: i32,
    config: Vec<(String, String)>,
}

struct AdminResource {
    client: AssertUnwindSafe<AdminClient<DefaultClientContext>>,
}

#[rustler::resource_impl]
impl rustler::Resource for AdminResource {}

#[rustler::nif(name = "admin_start")]
fn start(config: AdminConfig) -> Result<ResourceArc<AdminResource>, String> {
    let mut cfg = ClientConfig::new();
    cfg.set("bootstrap.servers", &config.bootstrap_servers);
    cfg.set_log_level(RDKafkaLogLevel::Debug);

    let client: AdminClient<DefaultClientContext> = cfg
        .create()
        .map_err(|e| format!("Failed to create Kafka admin client: {}", e))?;

    Ok(ResourceArc::new(AdminResource {
        client: AssertUnwindSafe(client),
    }))
}

#[rustler::task]
async fn create_topics(
    admin_resource: ResourceArc<AdminResource>,
    new_topics: Vec<NewTopic>,
) -> Result<Vec<Result<String, (String, String)>>, String> {
    let topics: Vec<rdkafka::admin::NewTopic> = new_topics
        .iter()
        .map(|new_topic| {
            let mut topic = rdkafka::admin::NewTopic::new(
                &new_topic.name,
                new_topic.num_partitions,
                rdkafka::admin::TopicReplication::Fixed(new_topic.replication),
            );

            for (k, v) in &new_topic.config {
                topic = topic.set(&k, &v);
            }

            topic
        })
        .collect();

    let admin_options = rdkafka::admin::AdminOptions::new();

    match admin_resource
        .client
        .0
        .create_topics(&topics, &admin_options)
        .await
    {
        Ok(results) => {
            let topic_results: Vec<Result<String, (String, String)>> = results
                .into_iter()
                .map(|result| match result {
                    Ok(topic) => Ok(topic.to_string()),
                    Err((topic, error)) => Err((topic.to_string(), error.to_string())),
                })
                .collect();
            Ok(topic_results)
        }
        Err(err) => Err(err.to_string()),
    }
}

#[rustler::task]
async fn delete_topics(
    admin_resource: ResourceArc<AdminResource>,
    topics: Vec<String>,
) -> Result<Vec<Result<String, (String, String)>>, String> {
    let topics: Vec<_> = topics.iter().map(|s| s.as_str()).collect();
    let admin_options = rdkafka::admin::AdminOptions::new();

    match admin_resource
        .client
        .0
        .delete_topics(&topics, &admin_options)
        .await
    {
        Ok(results) => {
            let topic_results: Vec<Result<String, (String, String)>> = results
                .into_iter()
                .map(|result| match result {
                    Ok(topic) => Ok(topic.to_string()),
                    Err((topic, error)) => Err((topic.to_string(), error.to_string())),
                })
                .collect();
            Ok(topic_results)
        }
        Err(err) => Err(err.to_string()),
    }
}

#[rustler::nif(name = "admin_stop")]
fn stop(_resource: ResourceArc<AdminResource>) -> Atom {
    // The admin client will be dropped when the resource is garbage collected
    ok()
}
