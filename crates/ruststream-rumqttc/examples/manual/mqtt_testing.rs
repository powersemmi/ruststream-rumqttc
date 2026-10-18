//! The service of `examples/mqtt_testing.rs` without the macros: the handler is a named type, the descriptor is mounted with `map_source`, and `main` is written by hand. The tests are the same: the harness drives the app whatever form it is written in.
//!
//! ```text
//! cargo run --example manual_mqtt_testing --features testing -- run
//! cargo test --example manual_mqtt_testing --features testing
//! ```

use std::process::ExitCode;

use ruststream::runtime::cli::run_main;
use ruststream::schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use ruststream_rumqttc::prelude::*;

#[derive(Debug, PartialEq, Deserialize, Serialize, JsonSchema)]
#[schemars(crate = "ruststream::schemars")]
struct Telemetry {
    device: String,
    temperature: f64,
}

// `#[derive(Outgoing)]` by hand: the type names no topic, so the publish names one.
impl OutgoingDestination for Telemetry {
    type Form = CallerName;
}

impl MessageHeaders for Telemetry {
    type Contract = NoHeaders;
}

#[derive(Debug, PartialEq, Deserialize, Serialize, JsonSchema)]
#[schemars(crate = "ruststream::schemars")]
struct Alert {
    device: String,
}

// `#[outgoing(name = "alerts")]` by hand: the type names the topic it is published to.
impl OutgoingDestination for Alert {
    type Form = FixedName;
    const DESTINATION: &'static str = "alerts";
}

impl MessageHeaders for Alert {
    type Contract = NoHeaders;
}

struct Check;

impl<Entry> Handle<Telemetry, (), Outs<(Entry,)>> for Check
where
    Entry: OutEntry<DefaultSlot, Wire: Publisher>,
{
    async fn handle(
        &self,
        telemetry: &Telemetry,
        outs: &Outs<(Entry,)>,
        _ctx: &mut Context<'_>,
    ) -> Result<(), HandlerOutcome> {
        if telemetry.temperature <= 30.0 {
            return Ok(());
        }
        let alert = Alert {
            device: telemetry.device.clone(),
        };
        if let Err(err) = outs.get(DefaultSlot).message(&alert).publish().await {
            tracing::warn!(subscription = "devices/+/telemetry", error = %err, "alert not published");
            return Err(HandlerOutcome::retry());
        }
        Ok(())
    }
}

fn telemetry() -> MqttFilter {
    MqttFilter::new("devices/+/telemetry").shared("workers")
}

/// The app `main` runs in production, and the one a test hands the harness unchanged.
fn app() -> impl App<State = ()> {
    RustStream::new(AppInfo::new("telemetry", "0.1.0")).with_broker(
        MqttBroker::new("mqtt://localhost:1883", "telemetry-svc"),
        |b| {
            let check = subscriber("devices/+/telemetry", Check).map_source(|_| telemetry());
            b.include(check.build())
                .out(DefaultSlot, Publish::default())
                .out_retry(Publish::default())
                .to("devices/retry/telemetry")
                .build();
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
    use ruststream_rumqttc::prelude::*;

    use super::{Alert, Telemetry, app};

    // --8<-- [start:testapp]
    #[tokio::test]
    async fn a_hot_reading_raises_an_alert() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;
        let reading = Telemetry {
            device: "dev42".to_owned(),
            temperature: 31.5,
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
        tb.broker::<MqttBroker>()
            .published::<Alert>("alerts")
            .assert_called_once()
            .with(&Alert {
                device: "dev42".to_owned(),
            });
        Ok(())
    }
    // --8<-- [end:testapp]

    // --8<-- [start:routing]
    #[tokio::test]
    async fn the_filter_matches_one_level_for_each_plus() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;
        let engine = Telemetry {
            device: "dev42-engine".to_owned(),
            temperature: 21.5,
        };
        let cabin = Telemetry {
            device: "dev42".to_owned(),
            temperature: 21.5,
        };
        // `+` stands for one level, so the first topic is outside the filter.
        tb.broker::<MqttBroker>()
            .message(&engine)
            .to("devices/dev42/engine/telemetry")
            .publish()
            .await?;
        tb.broker::<MqttBroker>()
            .message(&cabin)
            .to("devices/dev42/telemetry")
            .publish()
            .await?;

        tb.broker::<MqttBroker>()
            .subscriber("devices/+/telemetry")
            .assert_called_once()
            .with(&cabin);
        Ok(())
    }
    // --8<-- [end:routing]
}
