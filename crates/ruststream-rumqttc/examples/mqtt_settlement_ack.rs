//! Acknowledgement on MQTT: the outcome a handler returns becomes the PUBACK or PUBREC of the
//! delivery, and a dropped delivery is acknowledged too, because MQTT has no other final answer.
//!
//! ```text
//! cargo test --example mqtt_settlement_ack --features testing
//! cargo run --example mqtt_settlement_ack -- run
//! ```

use ruststream::schemars::JsonSchema;
use ruststream_rumqttc::prelude::*;
use serde::{Deserialize, Serialize};

/// A command sent to one device.
#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing, JsonSchema)]
#[schemars(crate = "ruststream::schemars")]
#[outgoing(name = "devices/dev42/commands")]
struct Command {
    action: String,
}

// --8<-- [start:handler]
/// The subscription reads at `QoS` 1, so returning from the handler acknowledges the delivery with
/// a PUBACK. A command the device does not know is dropped: on MQTT that acknowledges it as well,
/// and the server never sends it again.
#[subscriber(MqttTopic::new("devices/dev42/commands").qos(Qos::AtLeastOnce))]
async fn apply(command: &Command) -> HandlerOutcome {
    match command.action.as_str() {
        "reboot" | "reset" => {
            tracing::info!(command.action, "command applied");
            HandlerOutcome::ack()
        }
        unknown => {
            tracing::warn!(action = unknown, "unknown command dropped");
            HandlerOutcome::drop()
        }
    }
}

#[ruststream::app]
fn app() -> impl App {
    RustStream::new(AppInfo::new("device-agent", "0.1.0")).with_broker(
        MqttBroker::new("mqtt://localhost:1883", "device-agent-dev42"),
        |b| {
            b.include(apply);
        },
    )
}
// --8<-- [end:handler]

#[cfg(test)]
mod tests {
    use std::error::Error;

    use ruststream::testing::{Outcome, TestApp};
    use ruststream_rumqttc::prelude::*;

    use super::{Command, app};

    fn command(action: &str) -> Command {
        Command {
            action: action.to_owned(),
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_known_command_is_acknowledged_and_an_unknown_one_dropped()
    -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;
        tb.broker::<MqttBroker>()
            .message(&command("reboot"))
            .publish()
            .await?;
        tb.broker::<MqttBroker>()
            .message(&command("self-destruct"))
            .publish()
            .await?;
        tb.settle().await?;

        // Both answers settle the delivery, so neither comes back.
        assert_eq!(
            tb.broker::<MqttBroker>()
                .subscriber("devices/dev42/commands")
                .outcomes(),
            [Outcome::Ack, Outcome::Drop],
        );
        tb.shutdown().await?;
        Ok(())
    }
}
