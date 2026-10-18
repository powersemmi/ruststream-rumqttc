//! The quality of service of a subscription: each descriptor names the level it reads at, and a
//! delivery arrives at the lower of that level and the publisher's. The scenario is a test.
//!
//! ```text
//! cargo test --example mqtt_subscriptions_qos --features testing
//! cargo run --example mqtt_subscriptions_qos --features testing -- run
//! ```

use ruststream_rumqttc::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
struct Telemetry {
    device: String,
    temperature: f64,
}

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
#[outgoing(name = "devices/dev42/commands")]
struct Command {
    id: u64,
    open: bool,
}

// --8<-- [start:descriptor]
/// A device reports every few seconds, so a lost reading costs nothing: at most once.
#[subscriber(MqttFilter::new("devices/+/telemetry").qos(Qos::AtMostOnce))]
async fn sample(telemetry: &Telemetry) -> HandlerOutcome {
    tracing::info!(device = %telemetry.device, telemetry.temperature, "reading sampled");
    HandlerOutcome::ack()
}

/// A valve command must run once and never twice: exactly once.
#[subscriber(MqttTopic::new("devices/dev42/commands").qos(Qos::ExactlyOnce))]
async fn execute(command: &Command) -> HandlerOutcome {
    tracing::info!(command.id, command.open, "command executed");
    HandlerOutcome::ack()
}
// --8<-- [end:descriptor]

#[ruststream::app]
fn app() -> impl App {
    RustStream::new(AppInfo::new("devices", "0.1.0")).with_broker(
        MqttBroker::new("mqtt://localhost:1883", "devices-svc"),
        |b| {
            b.include(sample)
                .out_retry(Publish::default())
                .to("devices/retry/telemetry");
            b.include(execute);
        },
    )
}

#[cfg(test)]
mod tests {
    use std::error::Error;

    use ruststream::testing::TestApp;

    use super::*;

    #[tokio::test]
    async fn each_subscription_reads_at_its_own_level() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;
        let reading = Telemetry {
            device: "dev42".to_owned(),
            temperature: 21.5,
        };
        let command = Command { id: 7, open: true };

        tb.broker::<MqttBroker>()
            .message(&reading)
            .to("devices/dev42/telemetry")
            .publish()
            .await?;
        tb.broker::<MqttBroker>()
            .message(&command)
            .publish()
            .await?;

        tb.broker::<MqttBroker>()
            .subscriber("devices/+/telemetry")
            .assert_called_once()
            .with(&reading)
            .settled(HandlerOutcome::ack());
        tb.broker::<MqttBroker>()
            .subscriber("devices/dev42/commands")
            .assert_called_once()
            .with(&command)
            .settled(HandlerOutcome::ack());
        Ok(())
    }
}
