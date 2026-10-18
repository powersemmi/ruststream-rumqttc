//! Overlapping filters on one connection: a message whose topic both filters match reaches each
//! subscription once. The scenario is a test.
//!
//! ```text
//! cargo test --example mqtt_subscriptions_overlaps --features testing
//! cargo run --example mqtt_subscriptions_overlaps --features testing -- run
//! ```

use ruststream_rumqttc::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
struct Telemetry {
    device: String,
    temperature: f64,
}

// --8<-- [start:handlers]
/// Records the readings of the whole fleet.
#[subscriber(MqttFilter::new("devices/+/telemetry"))]
async fn record(telemetry: &Telemetry) -> HandlerOutcome {
    tracing::info!(device = %telemetry.device, telemetry.temperature, "reading recorded");
    HandlerOutcome::ack()
}

/// Watches everything dev42 sends, its readings included.
#[subscriber(MqttFilter::new("devices/dev42/#"))]
async fn watch(telemetry: &Telemetry) -> HandlerOutcome {
    tracing::info!(device = %telemetry.device, "dev42 watched");
    HandlerOutcome::ack()
}

#[ruststream::app]
fn app() -> impl App {
    RustStream::new(AppInfo::new("telemetry", "0.1.0")).with_broker(
        MqttBroker::new("mqtt://localhost:1883", "telemetry-svc"),
        |b| {
            // Each retry topic matches its own filter and not the other one, so a copy comes
            // back to the subscription that asked for it alone.
            b.include(record)
                .out_retry(Publish::default())
                .to("devices/retry/telemetry");
            b.include(watch)
                .out_retry(Publish::default())
                .to("devices/dev42/retry");
        },
    )
}
// --8<-- [end:handlers]

#[cfg(test)]
mod tests {
    use std::error::Error;

    use ruststream::testing::TestApp;

    use super::*;

    fn reading(device: &str) -> Telemetry {
        Telemetry {
            device: device.to_owned(),
            temperature: 21.5,
        }
    }

    #[tokio::test]
    async fn each_overlapping_filter_receives_a_message_once() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;

        // Both filters match dev42's readings; only the fleet filter matches dev43's.
        for device in ["dev42", "dev43"] {
            tb.broker::<MqttBroker>()
                .message(&reading(device))
                .to(format!("devices/{device}/telemetry"))
                .publish()
                .await?;
        }

        tb.broker::<MqttBroker>()
            .subscriber("devices/+/telemetry")
            .assert_called(2)
            .settled(HandlerOutcome::ack());
        tb.broker::<MqttBroker>()
            .subscriber("devices/dev42/#")
            .assert_called_once()
            .with(&reading("dev42"))
            .settled(HandlerOutcome::ack());
        Ok(())
    }
}
