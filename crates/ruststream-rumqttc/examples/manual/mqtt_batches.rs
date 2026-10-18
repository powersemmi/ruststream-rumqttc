//! The service of `examples/mqtt_batches.rs` written without the macros: a body over `[T]` is a
//! batch handler, and the mount site names its size.
//!
//! ```text
//! cargo test --example manual_mqtt_batches --features testing
//! cargo run --example manual_mqtt_batches --features testing -- run
//! ```

use std::future::{Future, ready};
use std::process::ExitCode;

use ruststream::runtime::cli::run_main;
use ruststream::schemars::JsonSchema;
use ruststream_rumqttc::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing, JsonSchema)]
#[schemars(crate = "ruststream::schemars")]
struct Reading {
    device: String,
    temperature: f64,
}

// --8<-- [start:batches]
/// Stores the readings of a whole batch in one write; `Ok` settles every reading of it.
struct Ingest;

impl Handle<[Reading]> for Ingest {
    fn handle(
        &self,
        readings: &[Reading],
        _outs: &(),
        _ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<(), Vec<HandlerOutcome>>> {
        tracing::info!(count = readings.len(), "readings stored in one write");
        ready(Ok(()))
    }
}

fn app() -> impl App<State = ()> {
    RustStream::new(AppInfo::new("telemetry-batches", "0.1.0")).with_broker(
        MqttBroker::new("mqtt://localhost:1883", "telemetry-batches"),
        |b| {
            let fleet = MqttFilter::new("devices/+/telemetry")
                .qos(Qos::AtLeastOnce)
                .shared("workers");
            // At most 64 readings per call; a batch that has fewer closes 20 ms after its first.
            b.include(
                subscriber("devices/+/telemetry", Ingest)
                    .batch(nonzero!(64))
                    .build()
                    .map_source(|_| fleet),
            )
            .out_retry(Publish::default())
            .to("devices/retry/telemetry");
        },
    )
}
// --8<-- [end:batches]

// What `#[ruststream::app]` writes: the command line on a multi-threaded Tokio runtime.
fn main() -> ExitCode {
    run_main(app)
}

#[cfg(test)]
mod tests {
    use std::error::Error;

    use futures::future::join_all;
    use ruststream::testing::TestApp;

    use super::*;

    const FLEET: &str = "devices/+/telemetry";

    fn readings(count: u32) -> Vec<(String, Reading)> {
        (1..=count)
            .map(|n| {
                let device = format!("dev{n}");
                let topic = format!("devices/{device}/telemetry");
                (
                    topic,
                    Reading {
                        device,
                        temperature: 21.5,
                    },
                )
            })
            .collect()
    }

    /// Publishes every reading at once, so they wait for the subscription together.
    async fn publish_at_once(
        tb: &TestApp<()>,
        readings: &[(String, Reading)],
    ) -> Result<(), Box<dyn Error>> {
        let broker = tb.broker::<MqttBroker>();
        for published in join_all(
            readings
                .iter()
                .map(|(topic, reading)| broker.message(reading).to(topic).publish()),
        )
        .await
        {
            published?;
        }
        Ok(())
    }

    #[tokio::test]
    async fn readings_that_arrive_together_share_one_call() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;

        publish_at_once(&tb, &readings(3)).await?;
        tb.settle().await?;

        tb.broker::<MqttBroker>()
            .subscriber(FLEET)
            .assert_called_once()
            .assert_batch_sizes(&[3])
            .settled(HandlerOutcome::ack());
        Ok(())
    }

    #[tokio::test]
    async fn a_batch_closes_at_the_size_the_mount_site_names() -> Result<(), Box<dyn Error>> {
        let pairs = RustStream::new(AppInfo::new("telemetry-batches", "0.1.0")).with_broker(
            MqttBroker::new("mqtt://localhost:1883", "telemetry-batches"),
            |b| {
                b.include(
                    subscriber(FLEET, Ingest)
                        .batch(nonzero!(2))
                        .build()
                        .map_source(|_| MqttFilter::new(FLEET).shared("workers")),
                )
                .out_retry(Publish::default())
                .to("devices/retry/telemetry");
            },
        );
        let tb = TestApp::start(pairs).await?;

        publish_at_once(&tb, &readings(4)).await?;
        tb.settle().await?;

        tb.broker::<MqttBroker>()
            .subscriber(FLEET)
            .assert_batch_sizes(&[2, 2])
            .settled(HandlerOutcome::ack());
        Ok(())
    }
}
