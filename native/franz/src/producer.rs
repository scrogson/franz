use crate::atoms::ok;
use crate::config::ProducerConfig;
use crate::message::Message;
use rdkafka::producer::{FutureProducer, FutureRecord};
use rdkafka::config::{ClientConfig, RDKafkaLogLevel};
use rustler::{Atom, ResourceArc};
use std::panic::AssertUnwindSafe;
use std::time::Duration;
use tracing::{error, trace};

struct ProducerResource {
    producer: AssertUnwindSafe<FutureProducer>,
}

#[rustler::resource_impl]
impl rustler::Resource for ProducerResource {}

#[rustler::nif(name = "producer_start")]
fn start(config: ProducerConfig) -> Result<ResourceArc<ProducerResource>, String> {
    let mut cfg = ClientConfig::new();
    cfg.set("bootstrap.servers", &config.bootstrap_servers);
    cfg.set_log_level(RDKafkaLogLevel::Debug);

    let producer: FutureProducer = cfg
        .create()
        .map_err(|e| format!("Failed to create Kafka producer: {}", e))?;

    Ok(ResourceArc::new(ProducerResource {
        producer: AssertUnwindSafe(producer),
    }))
}

#[rustler::task]
async fn producer_send(
    producer_resource: ResourceArc<ProducerResource>,
    msg: Message,
) -> Result<(), String> {
    let topic = &msg.topic;
    let partition = Some(msg.partition);
    let key = msg.key.map(|k| k.0);
    let payload = msg.payload.map(|p| p.0);
    let timestamp = msg.timestamp;

    let record = FutureRecord {
        topic,
        partition,
        key: key.as_ref(),
        payload: payload.as_ref(),
        timestamp,
        headers: None,
    };

    trace!("Sending message to topic={}, partition={:?}", topic, partition);

    match producer_resource
        .producer
        .0
        .send(record, Duration::from_secs(5))
        .await
    {
        Ok(delivery) => {
            trace!(
                "Message sent successfully: partition={}, offset={}",
                delivery.partition,
                delivery.offset
            );
            Ok(())
        }
        Err((err, _)) => {
            error!("Failed to send message: {:?}", err);
            Err(format!("{:?}", err))
        }
    }
}

#[rustler::nif(name = "producer_stop")]
fn stop(_resource: ResourceArc<ProducerResource>) -> Atom {
    // The producer will be dropped when the resource is garbage collected
    ok()
}
