//! A filter descriptor: `MqttFilter` subscribes with the `+` and `#` wildcards, and the mount site
//! names the topic its retry copies go to. The scenarios are tests.
//!
//! ```text
//! cargo test --example mqtt_subscriptions_filter --features testing
//! cargo run --example mqtt_subscriptions_filter --features testing -- run
//! ```

use ruststream_rumqttc::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
struct Telemetry {
    device: String,
    temperature: f64,
}

// --8<-- [start:descriptor]
/// Collects the readings of every device: `+` stands for the device's own level of the topic.
#[subscriber(MqttFilter::new("devices/+/telemetry").qos(Qos::AtLeastOnce))]
async fn collect(telemetry: &Telemetry) -> HandlerOutcome {
    tracing::info!(device = %telemetry.device, telemetry.temperature, "reading collected");
    HandlerOutcome::ack()
}

#[ruststream::app]
fn app() -> impl App {
    RustStream::new(AppInfo::new("telemetry", "0.1.0")).with_broker(
        MqttBroker::new("mqtt://localhost:1883", "telemetry-svc"),
        |b| {
            // A filter is no topic to publish to, so the mount site names where a retry copy
            // goes: a topic the filter matches, so the copy comes back here.
            b.include(collect)
                .out_retry(Publish::default())
                .to("devices/retry/telemetry");
        },
    )
}
// --8<-- [end:descriptor]

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
    async fn every_device_topic_reaches_the_filter() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;

        for device in ["dev42", "dev43"] {
            tb.broker::<MqttBroker>()
                .message(&reading(device))
                .to(format!("devices/{device}/telemetry"))
                .publish()
                .await?;
        }
        // One level deeper than `+` reaches, so the filter does not match it.
        tb.broker::<MqttBroker>()
            .message(&reading("dev44"))
            .to("devices/dev44/telemetry/raw")
            .publish()
            .await?;

        let received = tb
            .broker::<MqttBroker>()
            .subscriber("devices/+/telemetry")
            .assert_called(2)
            .received::<Telemetry>();
        assert_eq!(received, [reading("dev42"), reading("dev43")]);
        Ok(())
    }

    #[tokio::test]
    async fn a_filter_that_names_no_retry_topic_does_not_start() {
        let unnamed = RustStream::new(AppInfo::new("telemetry", "0.1.0")).with_broker(
            MqttBroker::new("mqtt://localhost:1883", "telemetry-svc"),
            |b| {
                b.include(collect);
            },
        );

        let refused = TestApp::start(unnamed)
            .await
            .expect_err("a filter names no topic a retry copy could go to");
        assert!(refused.to_string().contains("MqttFilter"), "{refused}");
    }
}
