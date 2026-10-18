//! The service of `examples/mqtt_service.rs` without the macros: the handler is a named type, the
//! descriptor is mounted with `map_source`, and `main` is written by hand.
//!
//! ```text
//! just brokers-up
//! cargo run --example manual_mqtt_service --features testing -- run
//! cargo test --example manual_mqtt_service --features testing
//! ```

use std::future::{Future, ready};
use std::process::ExitCode;

use ruststream::runtime::cli::run_main;
use ruststream::schemars::JsonSchema;
use serde::{Deserialize, Serialize};

// --8<-- [start:service]
// --8<-- [start:handler]
use ruststream_rumqttc::prelude::*;

struct Record;

impl Handle<Telemetry> for Record {
    fn handle(
        &self,
        telemetry: &Telemetry,
        _outs: &(),
        _ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<(), HandlerOutcome>> {
        tracing::info!(telemetry.device, telemetry.temperature, "reading recorded");
        ready(Ok(()))
    }
}

fn telemetry() -> MqttFilter {
    MqttFilter::new("devices/+/telemetry").shared("workers")
}
// --8<-- [end:handler]

// --8<-- [start:app]
fn app() -> impl App {
    RustStream::new(AppInfo::new("telemetry", "0.1.0")).with_broker(
        MqttBroker::new("mqtt://localhost:1883", "telemetry-svc"),
        |b| {
            let record = subscriber("devices/+/telemetry", Record).map_source(|_| telemetry());
            // A filter matches many topics and names none of them, so the mount site says where
            // a retry copy is published.
            b.include(record.build())
                .out_retry(Publish::default())
                .to("devices/retry/telemetry");
        },
    )
}
// --8<-- [end:app]
// --8<-- [end:service]

// What `#[ruststream::app]` writes: the command line on a multi-threaded Tokio runtime.
fn main() -> ExitCode {
    run_main(app)
}

#[derive(Debug, PartialEq, Deserialize, Serialize, JsonSchema)]
#[schemars(crate = "ruststream::schemars")]
struct Telemetry {
    device: String,
    temperature: f64,
}

// `#[derive(Outgoing)]` by hand: a reading names no topic, so the publish names one.
impl OutgoingDestination for Telemetry {
    type Form = CallerName;
}

impl MessageHeaders for Telemetry {
    type Contract = NoHeaders;
}

#[cfg(test)]
mod tests {
    use std::error::Error;

    use ruststream::testing::TestApp;
    use ruststream_rumqttc::prelude::*;

    use super::{Telemetry, app};

    #[tokio::test]
    async fn a_reading_from_any_device_is_recorded() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;
        let reading = Telemetry {
            device: "dev42".to_owned(),
            temperature: 21.5,
        };
        tb.broker::<MqttBroker>()
            .message(&reading)
            .to("devices/dev42/telemetry")
            .publish()
            .await?;

        tb.broker::<MqttBroker>()
            .subscriber("devices/+/telemetry")
            .assert_called_once()
            .with(&reading)
            .settled(HandlerOutcome::ack());
        Ok(())
    }
}
