//! Testing a service in process: the production app, `MqttBroker` included, run by `TestApp` with
//! no MQTT server. A publish through the harness returns once every handler it woke has settled,
//! so the assertions read finished state.
//!
//! ```text
//! cargo run --example mqtt_testing --features testing -- run
//! cargo test --example mqtt_testing --features testing
//! ```

use serde::{Deserialize, Serialize};

use ruststream_rumqttc::prelude::*;

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
struct Telemetry {
    device: String,
    temperature: f64,
}

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
#[outgoing(name = "alerts")]
struct Alert {
    device: String,
}

#[subscriber(MqttFilter::new("devices/+/telemetry").shared("workers"))]
async fn check(telemetry: &Telemetry, Out(alerts): Out<impl Publisher>) -> HandlerOutcome {
    if telemetry.temperature <= 30.0 {
        return HandlerOutcome::ack();
    }
    let alert = Alert {
        device: telemetry.device.clone(),
    };
    if alerts.message(&alert).publish().await.is_err() {
        return HandlerOutcome::retry();
    }
    HandlerOutcome::ack()
}

/// The app `main` runs in production, and the one a test hands the harness unchanged.
#[ruststream::app]
fn app() -> impl App<State = ()> {
    RustStream::new(AppInfo::new("telemetry", "0.1.0")).with_broker(
        MqttBroker::new("mqtt://localhost:1883", "telemetry-svc"),
        |b| {
            b.include(check)
                .out(DefaultSlot, Publish::default())
                .out_retry(Publish::default())
                .to("devices/retry/telemetry")
                .build();
        },
    )
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
