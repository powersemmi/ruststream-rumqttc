//! The service of `examples/mqtt_subscriptions_filter.rs` written without the macros: the filter
//! joins the chain through `map_source`, and the mount site names the topic its retry copies go
//! to.
//!
//! ```text
//! cargo test --example manual_mqtt_subscriptions_filter --features testing
//! cargo run --example manual_mqtt_subscriptions_filter --features testing -- run
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

// --8<-- [start:descriptor]
/// Collects the readings of every device: `+` stands for the device's own level of the topic.
struct Collect;

impl Handle<Telemetry> for Collect {
    fn handle(
        &self,
        telemetry: &Telemetry,
        _outs: &(),
        _ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<(), HandlerOutcome>> {
        tracing::info!(device = %telemetry.device, telemetry.temperature, "reading collected");
        ready(Ok(()))
    }
}

fn app() -> impl App {
    RustStream::new(AppInfo::new("telemetry", "0.1.0")).with_broker(
        MqttBroker::new("mqtt://localhost:1883", "telemetry-svc"),
        |b| {
            let fleet = MqttFilter::new("devices/+/telemetry").qos(Qos::AtLeastOnce);
            // A filter is no topic to publish to, so the mount site names where a retry copy
            // goes: a topic the filter matches, so the copy comes back here.
            b.include(
                subscriber("devices/+/telemetry", Collect)
                    .build()
                    .map_source(|_| fleet),
            )
            .out_retry(Publish::default())
            .to("devices/retry/telemetry");
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
                let fleet = MqttFilter::new("devices/+/telemetry");
                b.include(
                    subscriber("devices/+/telemetry", Collect)
                        .build()
                        .map_source(|_| fleet),
                );
            },
        );

        let refused = TestApp::start(unnamed)
            .await
            .expect_err("a filter names no topic a retry copy could go to");
        assert!(refused.to_string().contains("MqttFilter"), "{refused}");
    }
}
