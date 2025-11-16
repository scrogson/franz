use crate::atoms::ok;
use crate::config::ProducerConfig;
use crate::message::{DeliveryReceipt, Message};
use rdkafka::config::ClientConfig;
use rdkafka::message::OwnedHeaders;
use rdkafka::producer::{FutureProducer, FutureRecord, Producer};
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
    let cfg: ClientConfig = config.into();

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
) -> Result<DeliveryReceipt, String> {
    let topic = msg.topic.clone();
    let partition = Some(msg.partition);
    let key = msg.key.map(|k| k.0);
    let payload = msg.payload.map(|p| p.0);
    let timestamp = msg.timestamp;

    let headers = if msg.headers.is_empty() {
        None
    } else {
        let mut owned_headers = OwnedHeaders::new();
        for (k, v) in msg.headers.iter() {
            owned_headers = owned_headers.insert(rdkafka::message::Header {
                key: k,
                value: Some(v.as_bytes()),
            });
        }
        Some(owned_headers)
    };

    let record = FutureRecord {
        topic: &topic,
        partition,
        key: key.as_ref(),
        payload: payload.as_ref(),
        timestamp,
        headers,
    };

    trace!(
        "Sending message to topic={}, partition={:?}",
        topic,
        partition
    );

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

            let receipt = DeliveryReceipt {
                topic,
                partition: delivery.partition,
                offset: delivery.offset,
                timestamp: delivery.timestamp.to_millis(),
            };

            Ok(receipt)
        }
        Err((err, _)) => {
            error!("Failed to send message: {:?}", err);
            Err(format!("{:?}", err))
        }
    }
}

/// Fire-and-forget send: enqueues the message without waiting for delivery.
/// Much faster but provides no delivery guarantees. Use flush() to ensure delivery.
#[rustler::task]
async fn producer_send_async(
    producer_resource: ResourceArc<ProducerResource>,
    msg: Message,
) -> Result<(), String> {
    use rdkafka::util::Timeout;

    let topic = msg.topic.clone();
    let partition = msg.partition;
    let key = msg.key.map(|k| k.0);
    let payload = msg.payload.map(|p| p.0);
    let timestamp = msg.timestamp;

    let headers = if msg.headers.is_empty() {
        None
    } else {
        let mut owned_headers = OwnedHeaders::new();
        for (k, v) in msg.headers.iter() {
            owned_headers = owned_headers.insert(rdkafka::message::Header {
                key: k,
                value: Some(v.as_bytes()),
            });
        }
        Some(owned_headers)
    };

    trace!(
        "Sending async message to topic={}, partition={:?}",
        topic,
        partition
    );

    // Spawn the send operation in the background and return immediately
    // This allows the Elixir side to continue without waiting for delivery
    let producer = producer_resource.producer.0.clone();
    tokio::spawn(async move {
        // Build the record inside the async block so all data is owned
        let record = FutureRecord {
            topic: &topic,
            partition: Some(partition),
            key: key.as_ref(),
            payload: payload.as_ref(),
            timestamp,
            headers,
        };

        match producer
            .send(record, Timeout::After(Duration::from_secs(5)))
            .await
        {
            Ok(_) => trace!("Async message delivered successfully"),
            Err((err, _)) => error!("Async message delivery failed: {:?}", err),
        }
    });

    // Return immediately without waiting for the spawned task
    Ok(())
}

#[rustler::task]
async fn producer_flush(
    producer_resource: ResourceArc<ProducerResource>,
    timeout_ms: i64,
) -> Result<(), String> {
    let timeout = Duration::from_millis(timeout_ms as u64);

    trace!("Flushing producer with timeout {:?}", timeout);

    producer_resource
        .producer
        .0
        .flush(timeout)
        .map_err(|e| format!("Failed to flush producer: {:?}", e))?;

    trace!("Producer flushed successfully");
    Ok(())
}

/// Get the number of messages waiting to be sent or acknowledged.
#[rustler::nif(name = "producer_in_flight_count")]
fn in_flight_count(producer_resource: ResourceArc<ProducerResource>) -> i32 {
    producer_resource.producer.0.in_flight_count()
}

#[rustler::nif(name = "producer_stop")]
fn stop(_resource: ResourceArc<ProducerResource>) -> Atom {
    // The producer will be dropped when the resource is garbage collected
    ok()
}
