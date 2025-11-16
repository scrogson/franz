use crate::atoms::ok;
use crate::config::AdminConfig;
use rdkafka::admin::AdminClient;
use rdkafka::client::DefaultClientContext;
use rdkafka::config::{ClientConfig, RDKafkaLogLevel};
use rustler::{Atom, NifStruct, NifTaggedEnum, ResourceArc};
use std::panic::AssertUnwindSafe;
use std::sync::Arc;

/// Replication configuration for a new topic.
#[derive(NifTaggedEnum, Clone)]
pub enum TopicReplication {
    /// All partitions will have the same replication factor.
    Fixed { factor: i32 },
    /// Each partition will have specific replica assignments.
    /// The outer vec corresponds to partitions (by index),
    /// and the inner vec specifies broker IDs for replicas.
    Variable { assignments: Vec<Vec<i32>> },
}

#[derive(NifStruct, Clone)]
#[module = "Franz.NewTopic"]
/// Configuration for a CreateTopic operation.
pub struct NewTopic {
    name: String,
    num_partitions: i32,
    replication: TopicReplication,
    config: Vec<(String, String)>,
}

struct AdminResource {
    client: AssertUnwindSafe<AdminClient<DefaultClientContext>>,
    config: Arc<ClientConfig>,
}

#[rustler::resource_impl]
impl rustler::Resource for AdminResource {}

#[rustler::nif(name = "admin_start")]
fn start(config: AdminConfig) -> Result<ResourceArc<AdminResource>, String> {
    let mut cfg = ClientConfig::new();
    cfg.set("bootstrap.servers", &config.bootstrap_servers);
    cfg.set_log_level(RDKafkaLogLevel::Warning);

    if let Some(security) = &config.security {
        security.apply_to_config(&mut cfg);
    }

    let client: AdminClient<DefaultClientContext> = cfg
        .create()
        .map_err(|e| format!("Failed to create Kafka admin client: {}", e))?;

    Ok(ResourceArc::new(AdminResource {
        client: AssertUnwindSafe(client),
        config: Arc::new(cfg),
    }))
}

#[rustler::task(name = "create_topics")]
async fn create_topics(
    admin_resource: ResourceArc<AdminResource>,
    new_topics: Vec<NewTopic>,
) -> Result<Vec<Result<String, (String, String)>>, String> {
    // Pre-process variable assignments to ensure proper lifetimes
    let assignment_storage: Vec<Vec<Vec<i32>>> = new_topics
        .iter()
        .map(|new_topic| {
            if let TopicReplication::Variable { assignments } = &new_topic.replication {
                assignments.clone()
            } else {
                vec![]
            }
        })
        .collect();

    let assignment_refs: Vec<Vec<&[i32]>> = assignment_storage
        .iter()
        .map(|assignments| {
            assignments
                .iter()
                .map(|v| v.as_slice())
                .collect()
        })
        .collect();

    let topics: Vec<rdkafka::admin::NewTopic> = new_topics
        .iter()
        .enumerate()
        .map(|(idx, new_topic)| {
            let rdkafka_replication = match &new_topic.replication {
                TopicReplication::Fixed { factor } => {
                    rdkafka::admin::TopicReplication::Fixed(*factor)
                }
                TopicReplication::Variable { .. } => {
                    rdkafka::admin::TopicReplication::Variable(&assignment_refs[idx])
                }
            };

            let mut topic = rdkafka::admin::NewTopic::new(
                &new_topic.name,
                new_topic.num_partitions,
                rdkafka_replication,
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

#[rustler::task(name = "delete_topics")]
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

#[rustler::task(name = "list_topics")]
async fn list_topics(
    admin_resource: ResourceArc<AdminResource>,
    timeout_ms: i32,
) -> Result<Vec<String>, String> {
    use rdkafka::consumer::{BaseConsumer, Consumer};
    use std::time::Duration;

    // Create a temporary consumer to fetch metadata
    let consumer: BaseConsumer = (*admin_resource.config)
        .create()
        .map_err(|e| format!("Failed to create consumer for metadata: {}", e))?;

    let metadata = consumer
        .fetch_metadata(None, Duration::from_millis(timeout_ms as u64))
        .map_err(|e| format!("Failed to fetch metadata: {}", e))?;

    let topics: Vec<String> = metadata
        .topics()
        .iter()
        .map(|t| t.name().to_string())
        .collect();

    Ok(topics)
}

#[rustler::task(name = "describe_cluster")]
async fn describe_cluster(
    admin_resource: ResourceArc<AdminResource>,
    timeout_ms: i32,
) -> Result<ClusterMetadata, String> {
    use rdkafka::consumer::{BaseConsumer, Consumer};
    use std::time::Duration;

    // Create a temporary consumer to fetch metadata
    let consumer: BaseConsumer = (*admin_resource.config)
        .create()
        .map_err(|e| format!("Failed to create consumer for metadata: {}", e))?;

    let metadata = consumer
        .fetch_metadata(None, Duration::from_millis(timeout_ms as u64))
        .map_err(|e| format!("Failed to fetch metadata: {}", e))?;

    let brokers: Vec<BrokerMetadata> = metadata
        .brokers()
        .iter()
        .map(|b| BrokerMetadata {
            id: b.id(),
            host: b.host().to_string(),
            port: b.port(),
        })
        .collect();

    Ok(ClusterMetadata {
        cluster_id: metadata.orig_broker_name().to_string(),
        controller_id: -1, // Controller ID not available from basic metadata
        broker_count: brokers.len() as i32,
        brokers,
    })
}

#[derive(NifStruct)]
#[module = "Franz.BrokerMetadata"]
pub struct BrokerMetadata {
    pub id: i32,
    pub host: String,
    pub port: i32,
}

#[derive(NifStruct)]
#[module = "Franz.ClusterMetadata"]
pub struct ClusterMetadata {
    pub cluster_id: String,
    pub controller_id: i32,
    pub broker_count: i32,
    pub brokers: Vec<BrokerMetadata>,
}

/// Configuration for adding partitions to an existing topic.
#[derive(NifStruct, Clone)]
#[module = "Franz.NewPartitions"]
pub struct NewPartitions {
    /// The topic name
    name: String,
    /// Total number of partitions after the operation completes
    total_count: i32,
    /// Optional replica assignments for new partitions only
    /// Each inner vec specifies broker IDs for that new partition's replicas
    assignment: Option<Vec<Vec<i32>>>,
}

#[rustler::task(name = "create_partitions")]
async fn create_partitions(
    admin_resource: ResourceArc<AdminResource>,
    new_partitions: Vec<NewPartitions>,
) -> Result<Vec<Result<String, (String, String)>>, String> {
    // Pre-process assignments to ensure proper lifetimes
    let assignment_storage: Vec<Option<Vec<Vec<i32>>>> = new_partitions
        .iter()
        .map(|np| np.assignment.clone())
        .collect();

    let assignment_refs: Vec<Option<Vec<&[i32]>>> = assignment_storage
        .iter()
        .map(|opt_assignments| {
            opt_assignments.as_ref().map(|assignments| {
                assignments
                    .iter()
                    .map(|v| v.as_slice())
                    .collect()
            })
        })
        .collect();

    let partitions: Vec<rdkafka::admin::NewPartitions> = new_partitions
        .iter()
        .enumerate()
        .map(|(idx, np)| {
            let mut new_parts = rdkafka::admin::NewPartitions::new(
                &np.name,
                np.total_count as usize,
            );

            if let Some(ref assignment) = assignment_refs[idx] {
                new_parts = new_parts.assign(assignment.as_slice());
            }

            new_parts
        })
        .collect();

    let admin_options = rdkafka::admin::AdminOptions::new();

    match admin_resource
        .client
        .0
        .create_partitions(&partitions, &admin_options)
        .await
    {
        Ok(results) => {
            let partition_results: Vec<Result<String, (String, String)>> = results
                .into_iter()
                .map(|result| match result {
                    Ok(topic) => Ok(topic.to_string()),
                    Err((topic, error)) => Err((topic.to_string(), error.to_string())),
                })
                .collect();
            Ok(partition_results)
        }
        Err(err) => Err(err.to_string()),
    }
}

#[rustler::nif(name = "admin_stop")]
fn stop(_resource: ResourceArc<AdminResource>) -> Atom {
    // The admin client will be dropped when the resource is garbage collected
    ok()
}
