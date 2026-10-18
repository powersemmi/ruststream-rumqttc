//! The service of `examples/mqtt_context.rs` written without `#[subscriber]`: a body typed over
//! `MqttContext` reads the topic off its context by the `DeliveryTopic` key.
//!
//! ```text
//! cargo test --example manual_mqtt_context --features testing
//! cargo run --example manual_mqtt_context --features testing -- run
//! ```

use std::future::{Future, ready};
use std::process::ExitCode;

use ruststream::runtime::cli::run_main;
use ruststream::schemars::JsonSchema;
use ruststream_rumqttc::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing, JsonSchema)]
#[schemars(crate = "ruststream::schemars")]
struct Alarm {
    code: u32,
}

/// An alarm, with the device that raised it.
#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing, JsonSchema)]
#[schemars(crate = "ruststream::schemars")]
#[outgoing(name = "incidents")]
struct Incident {
    device: String,
    code: u32,
}

// --8<-- [start:handler]
/// The alarm itself names no device: its topic does, at the level the filter's `+` matched.
struct OpenIncident;

impl Handle<Alarm, Incident, (), MqttContext> for OpenIncident {
    fn handle(
        &self,
        alarm: &Alarm,
        _outs: &(),
        ctx: &mut Context<'_, MqttContext>,
    ) -> impl Future<Output = Result<Incident, HandlerOutcome>> {
        let topic = ctx.context(DeliveryTopic);
        let device = topic.split('/').nth(1).unwrap_or_default().to_owned();
        tracing::info!(%topic, alarm.code, "incident opened");
        ready(Ok(Incident {
            device,
            code: alarm.code,
        }))
    }
}
// --8<-- [end:handler]

fn app() -> impl App {
    RustStream::new(AppInfo::new("incidents", "0.1.0")).with_broker(
        MqttBroker::new("mqtt://localhost:1883", "incidents-svc"),
        |b| {
            let alarms = subscriber("devices/+/alarms", OpenIncident)
                .map_source(|_| MqttFilter::new("devices/+/alarms"));
            b.include(alarms.reply().build())
                .out_reply(Publish::default())
                .out_retry(Publish::default())
                .to("devices/retry/alarms");
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

    use super::*;

    #[tokio::test]
    async fn the_incident_names_the_device_of_the_topic() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;

        tb.broker::<MqttBroker>()
            .message(&Alarm { code: 3 })
            .to("devices/dev42/alarms")
            .publish()
            .await?;

        tb.broker::<MqttBroker>()
            .published::<Incident>("incidents")
            .assert_called_once()
            .with(&Incident {
                device: "dev42".to_owned(),
                code: 3,
            });
        Ok(())
    }
}
