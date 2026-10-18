//! A batch handler over device telemetry: the mount site names the batch size, and the crate
//! fills the batches on the client. The scenarios are tests.
//!
//! ```text
//! cargo test --example mqtt_batches --features testing
//! cargo run --example mqtt_batches --features testing -- run
//! ```

use ruststream_rumqttc::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
struct Reading {
    device: String,
    temperature: f64,
}

// --8<-- [start:batches]
/// Stores the readings of a whole batch in one write; one outcome settles every reading of it.
#[subscriber(MqttFilter::new("devices/+/telemetry").qos(Qos::AtLeastOnce).shared("workers"))]
async fn ingest(readings: &[Reading]) -> HandlerOutcome {
    tracing::info!(count = readings.len(), "readings stored in one write");
    HandlerOutcome::ack()
}

#[ruststream::app]
fn app() -> impl App<State = ()> {
    RustStream::new(AppInfo::new("telemetry-batches", "0.1.0")).with_broker(
        MqttBroker::new("mqtt://localhost:1883", "telemetry-batches"),
        |b| {
            // At most 64 readings per call; a batch that has fewer closes 20 ms after its first.
            b.include(ingest.batch(nonzero!(64)))
                .out_retry(Publish::default())
                .to("devices/retry/telemetry");
        },
    )
}
// --8<-- [end:batches]

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
                b.include(ingest.batch(nonzero!(2)))
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
