//! A subscription by topic name: `#[subscriber("devices/dev42/telemetry")]` subscribes to that
//! one topic at `QoS` 1. The scenarios are tests.
//!
//! ```text
//! cargo test --example mqtt_subscriptions_name --features testing
//! cargo run --example mqtt_subscriptions_name --features testing -- run
//! ```

use ruststream_rumqttc::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
#[outgoing(name = "devices/dev42/telemetry")]
struct Telemetry {
    device: String,
    temperature: f64,
}

// --8<-- [start:handler]
/// Subscribes to the one topic dev42 publishes its readings to.
#[subscriber("devices/dev42/telemetry")]
async fn record(telemetry: &Telemetry) -> HandlerOutcome {
    tracing::info!(device = %telemetry.device, telemetry.temperature, "reading recorded");
    HandlerOutcome::ack()
}

#[ruststream::app]
fn app() -> impl App {
    RustStream::new(AppInfo::new("telemetry", "0.1.0")).with_broker(
        MqttBroker::new("mqtt://localhost:1883", "telemetry-svc"),
        |b| {
            b.include(record);
        },
    )
}
// --8<-- [end:handler]

#[cfg(test)]
mod tests {
    use std::error::Error;

    use ruststream::testing::TestApp;

    use super::*;

    /// A name with a wildcard is a filter, which the bare form does not take.
    #[subscriber("devices/+/telemetry")]
    async fn fleet(telemetry: &Telemetry) -> HandlerOutcome {
        tracing::info!(device = %telemetry.device, "fleet reading");
        HandlerOutcome::ack()
    }

    #[tokio::test]
    async fn a_reading_on_the_topic_reaches_the_handler() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;
        let reading = Telemetry {
            device: "dev42".to_owned(),
            temperature: 21.5,
        };

        tb.broker::<MqttBroker>()
            .message(&reading)
            .publish()
            .await?;

        tb.broker::<MqttBroker>()
            .subscriber("devices/dev42/telemetry")
            .assert_called_once()
            .with(&reading)
            .settled(HandlerOutcome::ack());
        Ok(())
    }

    #[tokio::test]
    async fn a_wildcard_in_the_name_does_not_start() {
        let wildcard = RustStream::new(AppInfo::new("telemetry", "0.1.0")).with_broker(
            MqttBroker::new("mqtt://localhost:1883", "telemetry-svc"),
            |b| {
                b.include(fleet);
            },
        );

        let refused = TestApp::start(wildcard)
            .await
            .expect_err("a name is a topic, and a topic carries no wildcard");
        assert!(refused.to_string().contains("MqttFilter"), "{refused}");
    }
}
