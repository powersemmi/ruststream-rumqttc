//! A batch handler over device telemetry: the mount site names the batch size, and the crate
//! fills the batches.
//!
//! MQTT carries one message per PUBLISH packet, so there is no batch fetch to hand the size to
//! and the batches are assembled on the client. Nothing below says which side of the wire filled
//! them, which is what lets the same body run on a broker that batches natively.
//!
//! Run a broker first (`just brokers-up`), then:
//! `cargo run --example mqtt_batches`

// --8<-- [start:batches]
use ruststream_rumqttc::prelude::*;
use serde::Deserialize;

#[derive(Debug, Deserialize)]
struct Reading {
    device: String,
    temperature: f64,
}

/// One call per batch, and the batch is exactly what the subscription delivered: the runtime
/// never splits or merges one.
#[subscriber(MqttTopic::new("devices/+/telemetry").qos(Qos::AtLeastOnce).shared("workers"))]
async fn ingest(readings: &[Reading]) -> HandlerOutcome {
    for reading in readings {
        println!("{}: {}", reading.device, reading.temperature);
    }
    HandlerOutcome::ack()
}

#[ruststream::app]
fn app() -> impl App {
    RustStream::new(AppInfo::new("telemetry-batches", "0.1.0")).with_broker(
        MqttBroker::new("mqtt://localhost:1883", "telemetry-batches"),
        |b| {
            // The size is the one number a batch mount names. What closes a batch that never
            // reaches it - here, 20 milliseconds after its first delivery - is the crate's.
            b.include(ingest.batch(nonzero!(64)));
        },
    )
}
// --8<-- [end:batches]
