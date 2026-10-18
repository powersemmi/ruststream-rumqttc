//! A topic descriptor: `MqttTopic` subscribes to one topic with no wildcard, and a retry copy
//! published to that topic comes back to the subscription with nothing named at the mount site.
//! The scenarios are tests.
//!
//! ```text
//! cargo test --example mqtt_subscriptions_topic --features testing
//! cargo run --example mqtt_subscriptions_topic --features testing -- run
//! ```

use std::time::Duration;

use ruststream::runtime::RETRY_COUNT_HEADER;
use ruststream_rumqttc::prelude::*;
use serde::{Deserialize, Serialize};

/// How long the valve needs after a refused command before it takes the next one.
const SETTLE: Duration = Duration::from_secs(5);

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
#[outgoing(name = "devices/dev42/commands")]
struct Command {
    id: u64,
    open: bool,
}

// --8<-- [start:descriptor]
/// Runs the commands sent to dev42; the valve refuses the first attempt while it settles, so the
/// command comes back as a copy five seconds later.
#[subscriber(MqttTopic::new("devices/dev42/commands").qos(Qos::ExactlyOnce))]
async fn execute(command: &Command, ctx: &mut Context) -> HandlerOutcome {
    if ctx.headers().get_str(RETRY_COUNT_HEADER).is_none() {
        tracing::info!(command.id, "valve settling, trying again");
        return HandlerOutcome::retry_after(SETTLE);
    }
    tracing::info!(command.id, command.open, "command executed");
    HandlerOutcome::ack()
}

#[ruststream::app]
fn app() -> impl App {
    RustStream::new(AppInfo::new("valves", "0.1.0")).with_broker(
        MqttBroker::new("mqtt://localhost:1883", "valves-svc"),
        |b| {
            // The topic is where a copy goes, so the mount site names nothing.
            b.include(execute);
        },
    )
}
// --8<-- [end:descriptor]

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
