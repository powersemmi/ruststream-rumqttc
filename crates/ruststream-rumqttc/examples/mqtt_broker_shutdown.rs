//! Shutdown: the runtime lets the handlers finish, then the broker sends a clean `DISCONNECT`
//! after everything they queued. A publisher kept past that point reports the closed connection.
//!
//! ```text
//! cargo test --example mqtt_broker_shutdown --features testing
//! ```

use std::time::Duration;

use serde::{Deserialize, Serialize};

use ruststream_rumqttc::prelude::*;

#[subscriber("broker-shutdown/telemetry")]
async fn check(telemetry: &Telemetry, Out(alerts): Out<impl Publisher>) -> HandlerOutcome {
    if telemetry.temperature <= 30.0 {
        return HandlerOutcome::ack();
    }
    let alert = Alert {
        device: telemetry.device.clone(),
    };
    if let Err(err) = alerts.message(&alert).publish().await {
        tracing::warn!(subscription = "broker-shutdown/telemetry", error = %err, "alert not published");
        return HandlerOutcome::retry();
    }
    HandlerOutcome::ack()
}

// --8<-- [start:app]
/// At shutdown the handlers get ten seconds to finish; their alerts and acknowledgements leave
/// before the broker disconnects.
fn service(broker: MqttBroker) -> impl App<State = ()> {
    RustStream::new(AppInfo::new("telemetry", "0.1.0"))
        .shutdown_timeout(Duration::from_secs(10))
        .with_broker(broker, |b| {
            b.include(check)
                .out(DefaultSlot, Publish::default())
                .build();
        })
}

#[ruststream::app]
fn app() -> impl App<State = ()> {
    service(MqttBroker::new("mqtt://localhost:1883", "telemetry-svc"))
}
// --8<-- [end:app]

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
#[outgoing(name = "broker-shutdown/telemetry")]
struct Telemetry {
    device: String,
    temperature: f64,
}

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
#[outgoing(name = "broker-shutdown/alerts")]
struct Alert {
    device: String,
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
