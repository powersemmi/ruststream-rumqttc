//! The service of `examples/mqtt_settlement_ack.rs` written without `#[subscriber]`: the handler
//! implements `Handle`, and an `Err` outcome settles the delivery as the macro form's return does.
//!
//! ```text
//! cargo test --example manual_mqtt_settlement_ack --features testing
//! cargo run --example manual_mqtt_settlement_ack -- run
//! ```

use std::future::{Future, ready};
use std::process::ExitCode;

use ruststream::runtime::cli::run_main;
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
/// The subscription reads at QoS 1, so returning from the handler acknowledges the delivery with
/// a PUBACK. A command the device does not know is dropped: on MQTT that acknowledges it as well,
/// and the server never sends it again.
struct Apply;

impl Handle<Command> for Apply {
    fn handle(
        &self,
        command: &Command,
        _outs: &(),
        _ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<(), HandlerOutcome>> {
        let verdict = match command.action.as_str() {
            "reboot" | "reset" => {
                tracing::info!(command.action, "command applied");
                Ok(())
            }
            unknown => {
                tracing::warn!(action = unknown, "unknown command dropped");
                Err(HandlerOutcome::drop())
            }
        };
        ready(verdict)
    }
}

fn app() -> impl App {
    RustStream::new(AppInfo::new("device-agent", "0.1.0")).with_broker(
        MqttBroker::new("mqtt://localhost:1883", "device-agent-dev42"),
        |b| {
            let commands = MqttTopic::new("devices/dev42/commands").qos(Qos::AtLeastOnce);
            b.include(
                subscriber("devices/dev42/commands", Apply)
                    .map_source(|_| commands)
                    .build(),
            );
        },
    )
}
// --8<-- [end:handler]

// What `#[ruststream::app]` writes: the command line on a multi-threaded Tokio runtime.
fn main() -> ExitCode {
    run_main(app)
}

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
