//! The service of `examples/mqtt_broker_errors.rs` without the macros: the handler is a named type and `main` is written by hand.
//!
//! ```text
//! cargo test --example manual_mqtt_broker_errors --features testing
//! ```

use std::process::ExitCode;

use ruststream::runtime::cli::run_main;
use ruststream::schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use ruststream_rumqttc::prelude::*;

/// An alert names no topic: each one goes to the topic of its device.
#[derive(Debug, PartialEq, Deserialize, Serialize, JsonSchema)]
#[schemars(crate = "ruststream::schemars")]
struct Alert {
    temperature: f64,
}

// `#[derive(Outgoing)]` by hand: the type names no topic, so the publish names one.
impl OutgoingDestination for Alert {
    type Form = CallerName;
}

impl MessageHeaders for Alert {
    type Contract = NoHeaders;
}

// --8<-- [start:handler]
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
            temperature: telemetry.temperature,
        };
        let topic = format!("alerts/{}", telemetry.device);
        if let Err(err) = outs
            .get(DefaultSlot)
            .message(&alert)
            .to(&topic)
            .publish()
            .await
        {
            // A device id with `+` or `#` makes no topic, and no retry changes that.
            tracing::warn!(
                subscription = "broker-errors/telemetry",
                device = telemetry.device,
                error = %err,
                "alert not published"
            );
            return Err(HandlerOutcome::drop());
        }
        Ok(())
    }
}
// --8<-- [end:handler]

fn app() -> impl App {
    RustStream::new(AppInfo::new("telemetry", "0.1.0")).with_broker(
        MqttBroker::new("mqtt://localhost:1883", "telemetry-svc"),
        |b| {
            b.include(subscriber("broker-errors/telemetry", Check).build())
                .out(DefaultSlot, Publish::default())
                .build();
        },
    )
}

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

// `#[outgoing(name = "broker-errors/telemetry")]` by hand: the type names the topic it is published to.
impl OutgoingDestination for Telemetry {
    type Form = FixedName;
    const DESTINATION: &'static str = "broker-errors/telemetry";
}

impl MessageHeaders for Telemetry {
    type Contract = NoHeaders;
}

#[cfg(test)]
mod tests {
    use std::error::Error;

    use ruststream::testing::TestApp;
    use ruststream_rumqttc::prelude::*;

    use super::{Alert, Telemetry, app};

    fn hot(device: &str) -> Telemetry {
        Telemetry {
            device: device.to_owned(),
            temperature: 31.5,
        }
    }

    #[tokio::test]
    async fn an_alert_goes_to_the_topic_of_its_device() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;
        tb.broker::<MqttBroker>()
            .message(&hot("dev42"))
            .publish()
            .await?;

        tb.broker::<MqttBroker>()
            .subscriber("broker-errors/telemetry")
            .assert_called_once()
            .settled(HandlerOutcome::ack());
        tb.broker::<MqttBroker>()
            .published::<Alert>("alerts/dev42")
            .assert_called_once()
            .with(&Alert { temperature: 31.5 });
        Ok(())
    }

    #[tokio::test]
    async fn a_device_id_that_makes_no_topic_drops_the_reading() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;
        tb.broker::<MqttBroker>()
            .message(&hot("dev+42"))
            .publish()
            .await?;

        tb.broker::<MqttBroker>()
            .subscriber("broker-errors/telemetry")
            .assert_called_once()
            .settled(HandlerOutcome::drop());
        tb.broker::<MqttBroker>()
            .published::<Alert>("alerts/dev+42")
            .assert_not_called();
        Ok(())
    }
}
