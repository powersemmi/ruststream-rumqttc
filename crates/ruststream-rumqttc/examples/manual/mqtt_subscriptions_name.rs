//! The service of `examples/mqtt_subscriptions_name.rs` written without the macros: a topic in
//! `subscriber(..)` subscribes to that one topic at `QoS` 1.
//!
//! ```text
//! cargo test --example manual_mqtt_subscriptions_name --features testing
//! cargo run --example manual_mqtt_subscriptions_name --features testing -- run
//! ```

use std::future::{Future, ready};
use std::process::ExitCode;

use ruststream::runtime::cli::run_main;
use ruststream::schemars::JsonSchema;
use ruststream_rumqttc::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing, JsonSchema)]
#[schemars(crate = "ruststream::schemars")]
#[outgoing(name = "devices/dev42/telemetry")]
struct Telemetry {
    device: String,
    temperature: f64,
}

// --8<-- [start:handler]
/// Subscribes to the one topic dev42 publishes its readings to.
struct Record;

impl Handle<Telemetry> for Record {
    fn handle(
        &self,
        telemetry: &Telemetry,
        _outs: &(),
        _ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<(), HandlerOutcome>> {
        tracing::info!(device = %telemetry.device, telemetry.temperature, "reading recorded");
        ready(Ok(()))
    }
}

fn app() -> impl App {
    RustStream::new(AppInfo::new("telemetry", "0.1.0")).with_broker(
        MqttBroker::new("mqtt://localhost:1883", "telemetry-svc"),
        |b| {
            b.include(subscriber("devices/dev42/telemetry", Record).build());
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

    use ruststream::testing::TestApp;

    use super::*;

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
                b.include(subscriber("devices/+/telemetry", Record).build());
            },
        );

        let refused = TestApp::start(wildcard)
            .await
            .expect_err("a name is a topic, and a topic carries no wildcard");
        assert!(refused.to_string().contains("MqttFilter"), "{refused}");
    }
}
