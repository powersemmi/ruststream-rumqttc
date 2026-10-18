//! The service of `examples/mqtt_subscriptions_overlaps.rs` written without the macros: two
//! filters that overlap on dev42's readings, each mounted through `map_source`.
//!
//! ```text
//! cargo test --example manual_mqtt_subscriptions_overlaps --features testing
//! cargo run --example manual_mqtt_subscriptions_overlaps --features testing -- run
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

/// Records the readings of the whole fleet.
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

/// Watches everything dev42 sends, its readings included.
struct Watch;

impl Handle<Telemetry> for Watch {
    fn handle(
        &self,
        telemetry: &Telemetry,
        _outs: &(),
        _ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<(), HandlerOutcome>> {
        tracing::info!(device = %telemetry.device, "dev42 watched");
        ready(Ok(()))
    }
}

// --8<-- [start:handlers]
fn app() -> impl App {
    RustStream::new(AppInfo::new("telemetry", "0.1.0")).with_broker(
        MqttBroker::new("mqtt://localhost:1883", "telemetry-svc"),
        |b| {
            // Each retry topic matches its own filter and not the other one, so a copy comes
            // back to the subscription that asked for it alone.
            b.include(
                subscriber("devices/+/telemetry", Record)
                    .build()
                    .map_source(|_| MqttFilter::new("devices/+/telemetry")),
            )
            .out_retry(Publish::default())
            .to("devices/retry/telemetry");
            b.include(
                subscriber("devices/dev42/#", Watch)
                    .build()
                    .map_source(|_| MqttFilter::new("devices/dev42/#")),
            )
            .out_retry(Publish::default())
            .to("devices/dev42/retry");
        },
    )
}
// --8<-- [end:handlers]

// What `#[ruststream::app]` writes: the command line on a multi-threaded Tokio runtime.
fn main() -> ExitCode {
    run_main(app)
}

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
