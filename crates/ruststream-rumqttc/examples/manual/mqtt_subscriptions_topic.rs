//! The service of `examples/mqtt_subscriptions_topic.rs` written without the macros: the
//! descriptor joins the chain through `map_source`, and a retry copy published to its topic comes
//! back with nothing named at the mount site.
//!
//! ```text
//! cargo test --example manual_mqtt_subscriptions_topic --features testing
//! cargo run --example manual_mqtt_subscriptions_topic --features testing -- run
//! ```

use std::future::{Future, ready};
use std::process::ExitCode;
use std::time::Duration;

use ruststream::runtime::RETRY_COUNT_HEADER;
use ruststream::runtime::cli::run_main;
use ruststream::schemars::JsonSchema;
use ruststream_rumqttc::prelude::*;
use serde::{Deserialize, Serialize};

/// How long the valve needs after a refused command before it takes the next one.
const SETTLE: Duration = Duration::from_secs(5);

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing, JsonSchema)]
#[schemars(crate = "ruststream::schemars")]
#[outgoing(name = "devices/dev42/commands")]
struct Command {
    id: u64,
    open: bool,
}

// --8<-- [start:descriptor]
/// Runs the commands sent to dev42; the valve refuses the first attempt while it settles, so the
/// command comes back as a copy five seconds later.
struct Execute;

impl Handle<Command> for Execute {
    fn handle(
        &self,
        command: &Command,
        _outs: &(),
        ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<(), HandlerOutcome>> {
        if ctx.headers().get_str(RETRY_COUNT_HEADER).is_none() {
            tracing::info!(command.id, "valve settling, trying again");
            return ready(Err(HandlerOutcome::retry_after(SETTLE)));
        }
        tracing::info!(command.id, command.open, "command executed");
        ready(Ok(()))
    }
}

fn app() -> impl App {
    RustStream::new(AppInfo::new("valves", "0.1.0")).with_broker(
        MqttBroker::new("mqtt://localhost:1883", "valves-svc"),
        |b| {
            let commands = MqttTopic::new("devices/dev42/commands").qos(Qos::ExactlyOnce);
            // The topic is where a copy goes, so the mount site names nothing.
            b.include(
                subscriber("devices/dev42/commands", Execute)
                    .build()
                    .map_source(|_| commands),
            );
        },
    )
}
// --8<-- [end:descriptor]

// What `#[ruststream::app]` writes: the command line on a multi-threaded Tokio runtime.
fn main() -> ExitCode {
    run_main(app)
}

#[cfg(test)]
mod tests {
    use std::error::Error;

    use ruststream::testing::TestApp;

    use super::*;

    #[tokio::test(start_paused = true)]
    async fn the_copy_returns_to_the_topic() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;
        let command = Command { id: 7, open: true };

        tb.broker::<MqttBroker>()
            .message(&command)
            .publish()
            .await?;
        tb.broker::<MqttBroker>()
            .subscriber("devices/dev42/commands")
            .assert_called_once()
            .settled(HandlerOutcome::retry_after(SETTLE));

        tb.advance(SETTLE).await?;

        tb.broker::<MqttBroker>()
            .subscriber("devices/dev42/commands")
            .assert_called(2)
            .with(&command)
            .settled(HandlerOutcome::ack());
        tb.broker::<MqttBroker>()
            .published::<Command>("devices/dev42/commands")
            .assert_called(2)
            .with_header(RETRY_COUNT_HEADER, "1");
        Ok(())
    }
}
