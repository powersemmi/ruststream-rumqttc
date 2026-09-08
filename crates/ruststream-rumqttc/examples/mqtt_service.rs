//! A minimal MQTT service: a shared subscription over device telemetry.
//!
//! Run a broker first (`just brokers-up`), then:
//! `cargo run --example mqtt_service`

use std::time::Duration;

// --8<-- [start:handler]
use ruststream_rumqttc::prelude::*;
use serde::Deserialize;

#[derive(Debug, Deserialize)]
struct Telemetry {
    temperature: f64,
}

#[subscriber(MqttTopic::new("devices/+/telemetry").qos(Qos::AtLeastOnce).shared("workers"))]
async fn handle(telemetry: &Telemetry) -> HandlerOutcome {
    println!("temperature: {}", telemetry.temperature);
    HandlerOutcome::ack()
}
// --8<-- [end:handler]

// --8<-- [start:app]
#[ruststream::app]
fn app() -> impl App {
    RustStream::new(AppInfo::new("telemetry", "0.1.0")).with_broker(
        MqttBroker::new("mqtt://localhost:1883", "telemetry-svc")
            .keep_alive(Duration::from_secs(30))
            .clean_start(false)
            .session_expiry(Duration::from_secs(3600)),
        |b| {
            b.include(handle);
        },
    )
}
// --8<-- [end:app]
