//! The service of `examples/mqtt_broker_shutdown.rs` without the macros: the handler is a named type and `main` is written by hand.
//!
//! ```text
//! cargo test --example manual_mqtt_broker_shutdown --features testing
//! ```

use std::process::ExitCode;
use std::time::Duration;

use ruststream::runtime::cli::run_main;
use ruststream::schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use ruststream_rumqttc::prelude::*;

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
            tracing::warn!(subscription = "broker-shutdown/telemetry", error = %err, "alert not published");
            return Err(HandlerOutcome::retry());
        }
        Ok(())
    }
}

// --8<-- [start:app]
/// At shutdown the handlers get ten seconds to finish; their alerts and acknowledgements leave
/// before the broker disconnects.
fn service(broker: MqttBroker) -> impl App<State = ()> {
    RustStream::new(AppInfo::new("telemetry", "0.1.0"))
        .shutdown_timeout(Duration::from_secs(10))
        .with_broker(broker, |b| {
            b.include(subscriber("broker-shutdown/telemetry", Check).build())
                .out(DefaultSlot, Publish::default())
                .build();
        })
}

fn app() -> impl App<State = ()> {
    service(MqttBroker::new("mqtt://localhost:1883", "telemetry-svc"))
}
// --8<-- [end:app]

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

// `#[outgoing(name = "broker-shutdown/telemetry")]` by hand: the type names the topic it is published to.
impl OutgoingDestination for Telemetry {
    type Form = FixedName;
    const DESTINATION: &'static str = "broker-shutdown/telemetry";
}

impl MessageHeaders for Telemetry {
    type Contract = NoHeaders;
}

#[derive(Debug, PartialEq, Deserialize, Serialize, JsonSchema)]
#[schemars(crate = "ruststream::schemars")]
struct Alert {
    device: String,
}

// `#[outgoing(name = "broker-shutdown/alerts")]` by hand: the type names the topic it is published to.
impl OutgoingDestination for Alert {
    type Form = FixedName;
    const DESTINATION: &'static str = "broker-shutdown/alerts";
}

impl MessageHeaders for Alert {
    type Contract = NoHeaders;
}

#[cfg(test)]
mod tests {
    use std::error::Error;

    use ruststream::testing::TestApp;
    use ruststream_rumqttc::MqttError;
    use ruststream_rumqttc::prelude::*;

    use super::{Alert, Telemetry, app, service};

    #[tokio::test]
    async fn an_alert_leaves_before_the_service_stops() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;
        tb.broker::<MqttBroker>()
            .message(&Telemetry {
                device: "dev42".to_owned(),
                temperature: 31.5,
            })
            .publish()
            .await?;

        tb.broker::<MqttBroker>()
            .published::<Alert>("broker-shutdown/alerts")
            .assert_called_once()
            .with(&Alert {
                device: "dev42".to_owned(),
            });
        tb.shutdown().await?;
        Ok(())
    }

    #[tokio::test]
    async fn a_publisher_kept_past_shutdown_reports_the_closed_connection()
    -> Result<(), Box<dyn Error>> {
        let broker = MqttBroker::new("mqtt://localhost:1883", "telemetry-svc");
        let kept = broker.publisher();
        TestApp::start(service(broker)).await?.shutdown().await?;

        let alert = Alert {
            device: "dev42".to_owned(),
        };
        let failed = kept.message(&alert).publish().await.err();
        let failed = failed.ok_or("a publish succeeded against a closed connection")?;
        assert!(
            matches!(
                failed.source().and_then(|err| err.downcast_ref()),
                Some(MqttError::NotConnected)
            ),
            "{failed}"
        );
        Ok(())
    }
}
