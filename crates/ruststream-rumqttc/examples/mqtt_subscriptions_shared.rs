//! A share group: `shared(group)` subscribes `$share/<group>/<filter>`, and the server hands each
//! message to one member of the group. The scenario is a test.
//!
//! ```text
//! cargo test --example mqtt_subscriptions_shared --features testing
//! cargo run --example mqtt_subscriptions_shared --features testing -- run
//! ```

use ruststream_rumqttc::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
struct Telemetry {
    device: String,
    temperature: f64,
}

// --8<-- [start:descriptor]
/// Every instance of the service joins the group `workers`, so each reading is handled once.
#[subscriber(MqttFilter::new("devices/+/telemetry").shared("workers"))]
async fn process(telemetry: &Telemetry) -> HandlerOutcome {
    tracing::info!(device = %telemetry.device, telemetry.temperature, "reading processed");
    HandlerOutcome::ack()
}
// --8<-- [end:descriptor]

#[ruststream::app]
fn app() -> impl App {
    RustStream::new(AppInfo::new("telemetry", "0.1.0")).with_broker(
        MqttBroker::new("mqtt://localhost:1883", "telemetry-svc"),
        |b| {
            b.include(process)
                .out_retry(Publish::default())
                .to("devices/retry/telemetry");
        },
    )
}

#[cfg(test)]
mod tests {
    use std::error::Error;

    use ruststream::testing::TestApp;

    use super::*;

    const FLEET: &str = "devices/+/telemetry";

    fn reading(device: &str) -> Telemetry {
        Telemetry {
            device: device.to_owned(),
            temperature: 21.5,
        }
    }

    /// Reads every reading outside the group.
    #[subscriber(MqttFilter::new("devices/+/telemetry"))]
    async fn archive(telemetry: &Telemetry) -> HandlerOutcome {
        tracing::info!(device = %telemetry.device, "reading archived");
        HandlerOutcome::ack()
    }

    /// Two members of the group and a plain subscription on the same filter: the group takes one
    /// copy of each reading between its members, and the plain subscription takes its own.
    #[tokio::test]
    async fn a_group_takes_one_copy_of_each_reading() -> Result<(), Box<dyn Error>> {
        let fleet = RustStream::new(AppInfo::new("telemetry", "0.1.0")).with_broker(
            MqttBroker::new("mqtt://localhost:1883", "telemetry-svc"),
            |b| {
                for _ in 0..2 {
                    b.include(process)
                        .out_retry(Publish::default())
                        .to("devices/retry/telemetry");
                }
                b.include(archive)
                    .out_retry(Publish::default())
                    .to("devices/retry/telemetry");
            },
        );
        let tb = TestApp::start(fleet).await?;

        for device in ["dev1", "dev2", "dev3", "dev4"] {
            tb.broker::<MqttBroker>()
                .message(&reading(device))
                .to(format!("devices/{device}/telemetry"))
                .publish()
                .await?;
        }

        // The three subscriptions share the filter's name: four readings handled by the group,
        // and the same four by the plain subscription.
        tb.broker::<MqttBroker>()
            .subscriber(FLEET)
            .assert_called(8)
            .settled(HandlerOutcome::ack());
        Ok(())
    }
}
