//! The first service of the Getting started guide, written without the macros: the handler is a
//! type with an `impl Handle`, mounted with the `subscriber` constructor, and `main` is written by
//! hand.
//!
//! ```text
//! docker compose up -d
//! cargo run --example manual_guide_getting_started --features testing -- run
//! ```

use std::future::{Future, ready};
use std::process::ExitCode;

use ruststream::runtime::cli::run_main;
use ruststream::schemars::JsonSchema;
use ruststream_rumqttc::prelude::*;
use serde::Deserialize;

// --8<-- [start:service]
/// The coldest temperature a sensor can report; anything below it is a broken sensor.
const ABSOLUTE_ZERO: f64 = -273.15;

#[derive(Debug, PartialEq, Deserialize, JsonSchema)]
#[schemars(crate = "ruststream::schemars")]
struct Telemetry {
    device: String,
    temperature: f64,
}

struct Record;

impl Handle<Telemetry> for Record {
    fn handle(
        &self,
        telemetry: &Telemetry,
        _outs: &(),
        _ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<(), HandlerOutcome>> {
        if telemetry.temperature < ABSOLUTE_ZERO {
            tracing::warn!(telemetry.device, "impossible reading dropped");
            return ready(Err(HandlerOutcome::drop()));
        }
        tracing::info!(telemetry.device, telemetry.temperature, "reading recorded");
        ready(Ok(()))
    }
}

fn app() -> impl App {
    RustStream::new(AppInfo::new("telemetry", "0.1.0")).with_broker(
        MqttBroker::new("mqtt://localhost:1883", "telemetry-svc"),
        |b| {
            let fleet = MqttFilter::new("devices/+/telemetry").qos(Qos::AtLeastOnce);
            b.include(
                subscriber("devices/+/telemetry", Record)
                    .map_source(|_| fleet)
                    .build(),
            )
            .out_retry(Publish::default())
            .to("devices/retry/telemetry");
        },
    )
}

fn main() -> ExitCode {
    run_main(app)
}
// --8<-- [end:service]

#[cfg(test)]
mod tests {
    use std::error::Error;

    use ruststream::testing::TestApp;
    use ruststream_rumqttc::prelude::*;
    use serde::Serialize;

    use super::{Telemetry, app};

    /// A reading as a device publishes it.
    #[derive(Serialize)]
    struct Reading {
        device: String,
        temperature: f64,
    }

    impl OutgoingDestination for Reading {
        type Form = CallerName;
    }

    impl MessageHeaders for Reading {
        type Contract = NoHeaders;
    }

    impl MessageInfo for Reading {
        const NAME: &'static str = "Reading";
        const DESCRIPTION: Option<&'static str> = None;
    }

    fn reading(temperature: f64) -> Reading {
        Reading {
            device: "dev42".to_owned(),
            temperature,
        }
    }

    #[tokio::test]
    async fn a_reading_of_any_device_is_recorded() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;

        tb.broker::<MqttBroker>()
            .message(&reading(21.5))
            .to("devices/dev42/telemetry")
            .publish()
            .await?;

        tb.broker::<MqttBroker>()
            .subscriber("devices/+/telemetry")
            .assert_called_once()
            .with(&Telemetry {
                device: "dev42".to_owned(),
                temperature: 21.5,
            })
            .settled(HandlerOutcome::ack());
        tb.shutdown().await?;
        Ok(())
    }

    #[tokio::test]
    async fn an_impossible_reading_is_dropped() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;

        tb.broker::<MqttBroker>()
            .message(&reading(-300.0))
            .to("devices/dev42/telemetry")
            .publish()
            .await?;

        tb.broker::<MqttBroker>()
            .subscriber("devices/+/telemetry")
            .assert_called_once()
            .settled(HandlerOutcome::drop());
        tb.shutdown().await?;
        Ok(())
    }
}
