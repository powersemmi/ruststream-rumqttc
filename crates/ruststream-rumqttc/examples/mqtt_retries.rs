//! Delayed redelivery on MQTT: what a registration declares about its retries, and where the
//! copies go.
//!
//! MQTT has no delayed redelivery and counts no attempts, so `retry_after` runs on the
//! framework's fallback: the runtime acknowledges the original, waits, and publishes a copy
//! carrying the retry count. Which topic that copy goes to is what the two subscription
//! descriptors differ in.
//!
//! Run a broker first (`just brokers-up`), then:
//! `cargo run --example mqtt_retries -- run`

use std::time::Duration;

use ruststream_rumqttc::prelude::*;
use serde::Deserialize;

#[derive(Debug, Deserialize)]
struct Reading {
    device: String,
    temperature: f64,
}

/// How long a reading waits for the store to come back.
const BACKOFF: Duration = Duration::from_secs(5);

// --8<-- [start:handlers]
/// One topic, so a publish to it reaches this subscription again.
#[subscriber(MqttTopic::new("devices/dev42/telemetry").qos(Qos::AtLeastOnce))]
async fn store(reading: &Reading) -> HandlerOutcome {
    if reading.temperature.is_nan() {
        return HandlerOutcome::retry_after(BACKOFF);
    }
    println!("stored {}", reading.device);
    HandlerOutcome::ack()
}

/// A filter over the whole fleet, which is a name no publisher can use.
#[subscriber(MqttFilter::new("devices/+/telemetry").qos(Qos::AtLeastOnce).shared("workers"))]
async fn collect(reading: &Reading) -> HandlerOutcome {
    if reading.temperature.is_nan() {
        return HandlerOutcome::retry_after(BACKOFF);
    }
    println!("collected {}", reading.device);
    HandlerOutcome::ack()
}
// --8<-- [end:handlers]

#[ruststream::app]
fn app() -> impl App {
    RustStream::new(AppInfo::new("telemetry-retries", "0.1.0")).with_broker(
        MqttBroker::new("mqtt://localhost:1883", "telemetry-retries"),
        |b| {
            // --8<-- [start:declaration]
            // The cap counts deliveries, the first one included. A reading that spends them all
            // goes to the dead-letter topic, which lies outside every filter this service reads:
            // one the subscription matches would hand the reading straight back.
            b.include(store)
                .max_attempts(nonzero!(5u32))
                .dead_letter("dead/telemetry");
            // --8<-- [end:declaration]

            // --8<-- [start:named]
            // The same declaration over a filter subscription, plus the one thing a filter owes:
            // the topic its copies are published to. This one matches the filter, so a copy comes
            // back to this subscription.
            b.include(collect)
                .max_attempts(nonzero!(5u32))
                .dead_letter("dead/telemetry")
                .out_retry(Publish::default())
                .to("devices/retry/telemetry");
            // --8<-- [end:named]
        },
    )
}
