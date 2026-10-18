//! The service of `examples/mqtt_subscriptions_qos.rs` written without the macros: each
//! descriptor names the quality of service it reads at.
//!
//! ```text
//! cargo test --example manual_mqtt_subscriptions_qos --features testing
//! cargo run --example manual_mqtt_subscriptions_qos --features testing -- run
//! ```

use std::future::{Future, ready};
use std::process::ExitCode;

use ruststream::runtime::cli::run_main;
use ruststream::schemars::JsonSchema;
use ruststream_rumqttc::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing, JsonSchema)]
#[schemars(crate = "ruststream::schemars")]
struct Telemetry {
    device: String,
    temperature: f64,
}

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing, JsonSchema)]
#[schemars(crate = "ruststream::schemars")]
#[outgoing(name = "devices/dev42/commands")]
struct Command {
    id: u64,
    open: bool,
}

struct Sample;

impl Handle<Telemetry> for Sample {
    fn handle(
        &self,
        telemetry: &Telemetry,
        _outs: &(),
        _ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<(), HandlerOutcome>> {
        tracing::info!(device = %telemetry.device, telemetry.temperature, "reading sampled");
        ready(Ok(()))
    }
}

struct Execute;

impl Handle<Command> for Execute {
    fn handle(
        &self,
        command: &Command,
        _outs: &(),
        _ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<(), HandlerOutcome>> {
        tracing::info!(command.id, command.open, "command executed");
        ready(Ok(()))
    }
}

// --8<-- [start:descriptor]
/// A device reports every few seconds, so a lost reading costs nothing: at most once.
fn readings() -> MqttFilter {
    MqttFilter::new("devices/+/telemetry").qos(Qos::AtMostOnce)
}

/// A valve command must run once and never twice: exactly once.
fn commands() -> MqttTopic {
    MqttTopic::new("devices/dev42/commands").qos(Qos::ExactlyOnce)
}
// --8<-- [end:descriptor]

fn app() -> impl App {
    RustStream::new(AppInfo::new("devices", "0.1.0")).with_broker(
        MqttBroker::new("mqtt://localhost:1883", "devices-svc"),
        |b| {
            b.include(
                subscriber("devices/+/telemetry", Sample)
                    .build()
                    .map_source(|_| readings()),
            )
            .out_retry(Publish::default())
            .to("devices/retry/telemetry");
            b.include(
                subscriber("devices/dev42/commands", Execute)
                    .build()
                    .map_source(|_| commands()),
            );
        },
    )
}

// What `#[ruststream::app]` writes: the command line on a multi-threaded Tokio runtime.
fn main() -> ExitCode {
    run_main(app)
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
