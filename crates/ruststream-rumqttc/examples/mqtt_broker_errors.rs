//! `MqttError` where a service meets it: a publish the client refuses, logged with its cause.
//!
//! ```text
//! cargo test --example mqtt_broker_errors --features testing
//! ```

use serde::{Deserialize, Serialize};

use ruststream_rumqttc::prelude::*;

/// An alert names no topic: each one goes to the topic of its device.
#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
struct Alert {
    temperature: f64,
}

// --8<-- [start:handler]
#[subscriber("broker-errors/telemetry")]
async fn check(telemetry: &Telemetry, Out(alerts): Out<impl Publisher>) -> HandlerOutcome {
    if telemetry.temperature <= 30.0 {
        return HandlerOutcome::ack();
    }
    let alert = Alert {
        temperature: telemetry.temperature,
    };
    let topic = format!("alerts/{}", telemetry.device);
    if let Err(err) = alerts.message(&alert).to(&topic).publish().await {
        // A device id with `+` or `#` makes no topic, and no retry changes that.
        tracing::warn!(
            subscription = "broker-errors/telemetry",
            device = telemetry.device,
            error = %err,
            "alert not published"
        );
        return HandlerOutcome::drop();
    }
    HandlerOutcome::ack()
}
// --8<-- [end:handler]

#[ruststream::app]
fn app() -> impl App {
    RustStream::new(AppInfo::new("telemetry", "0.1.0")).with_broker(
        MqttBroker::new("mqtt://localhost:1883", "telemetry-svc"),
        |b| {
            b.include(check)
                .out(DefaultSlot, Publish::default())
                .build();
        },
    )
}

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
#[outgoing(name = "broker-errors/telemetry")]
struct Telemetry {
    device: String,
    temperature: f64,
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
