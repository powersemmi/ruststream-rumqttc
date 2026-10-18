//! The first service of the Getting started guide: one handler over the telemetry of every
//! device.
//!
//! ```text
//! docker compose up -d
//! cargo run --example guide_getting_started --features testing -- run
//! ```

use ruststream_rumqttc::prelude::*;
use serde::Deserialize;

// --8<-- [start:service]
/// The coldest temperature a sensor can report; anything below it is a broken sensor.
const ABSOLUTE_ZERO: f64 = -273.15;

#[derive(Debug, PartialEq, Deserialize)]
struct Telemetry {
    device: String,
    temperature: f64,
}

#[subscriber(MqttFilter::new("devices/+/telemetry").qos(Qos::AtLeastOnce))]
async fn record(telemetry: &Telemetry) -> HandlerOutcome {
    if telemetry.temperature < ABSOLUTE_ZERO {
        tracing::warn!(telemetry.device, "impossible reading dropped");
        return HandlerOutcome::drop();
    }
    tracing::info!(telemetry.device, telemetry.temperature, "reading recorded");
    HandlerOutcome::ack()
}

#[ruststream::app]
fn app() -> impl App {
    RustStream::new(AppInfo::new("telemetry", "0.1.0")).with_broker(
        MqttBroker::new("mqtt://localhost:1883", "telemetry-svc"),
        |b| {
            b.include(record)
                .out_retry(Publish::default())
                .to("devices/retry/telemetry");
        },
    )
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
    #[derive(Serialize, Outgoing)]
    struct Reading {
        device: String,
        temperature: f64,
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
