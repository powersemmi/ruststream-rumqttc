//! The service of `examples/mqtt_broker_prelude.rs` without the macros: the handler is a named type, the descriptor is mounted with `map_source`, and `main` is written by hand.
//!
//! ```text
//! cargo test --example manual_mqtt_broker_prelude --features testing
//! ```

use std::process::ExitCode;

use ruststream::runtime::cli::run_main;
use ruststream::schemars::JsonSchema;
use serde::{Deserialize, Serialize};

// --8<-- [start:app]
use ruststream_rumqttc::prelude::*;

struct Check;

impl<Entry> Handle<Telemetry, (), Outs<(Entry,)>, MqttContext> for Check
where
    Entry: OutEntry<DefaultSlot, Wire: Publisher>,
{
    async fn handle(
        &self,
        telemetry: &Telemetry,
        outs: &Outs<(Entry,)>,
        ctx: &mut Context<'_, MqttContext>,
    ) -> Result<(), HandlerOutcome> {
        if telemetry.temperature <= 30.0 {
            return Ok(());
        }
        let alert = Alert {
            topic: ctx.context(DeliveryTopic).to_owned(),
        };
        if outs
            .get(DefaultSlot)
            .message(&alert)
            .publish()
            .await
            .is_err()
        {
            return Err(HandlerOutcome::retry());
        }
        Ok(())
    }
}

fn telemetry() -> MqttFilter {
    MqttFilter::new("devices/+/telemetry").qos(Qos::AtLeastOnce)
}

fn app() -> impl App {
    RustStream::new(AppInfo::new("telemetry", "0.1.0")).with_broker(
        MqttBroker::new("mqtt://localhost:1883", "telemetry-svc"),
        |b| {
            let check = subscriber("devices/+/telemetry", Check).map_source(|_| telemetry());
            b.include(check.build())
                .out(DefaultSlot, Publish::default().qos(Qos::ExactlyOnce))
                .out_retry(Publish::default())
                .to("devices/retry/telemetry")
                .build();
        },
    )
}
// --8<-- [end:app]

// What `#[ruststream::app]` writes: the command line on a multi-threaded Tokio runtime.
fn main() -> ExitCode {
    run_main(app)
}

#[derive(Debug, PartialEq, Deserialize, Serialize, JsonSchema)]
#[schemars(crate = "ruststream::schemars")]
struct Telemetry {
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
    topic: String,
}

// `#[outgoing(name = "alerts")]` by hand: the type names the topic it is published to.
impl OutgoingDestination for Alert {
    type Form = FixedName;
    const DESTINATION: &'static str = "alerts";
}

impl MessageHeaders for Alert {
    type Contract = NoHeaders;
}

#[cfg(test)]
mod tests {
    use std::error::Error;

    use ruststream::testing::TestApp;
    use ruststream_rumqttc::prelude::*;

    use super::{Alert, Telemetry, app};

    #[tokio::test]
    async fn an_alert_names_the_topic_the_reading_arrived_on() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;
        tb.broker::<MqttBroker>()
            .message(&Telemetry { temperature: 31.5 })
            .to("devices/dev42/telemetry")
            .publish()
            .await?;

        tb.broker::<MqttBroker>()
            .published::<Alert>("alerts")
            .assert_called_once()
            .with(&Alert {
                topic: "devices/dev42/telemetry".to_owned(),
            });
        Ok(())
    }
}
