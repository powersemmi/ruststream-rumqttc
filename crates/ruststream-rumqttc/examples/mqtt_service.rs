//! A minimal MQTT service: a shared subscription over the telemetry of every device.
//!
//! ```text
//! just brokers-up
//! cargo run --example mqtt_service --features testing -- run
//! cargo test --example mqtt_service --features testing
//! ```

use serde::{Deserialize, Serialize};

// --8<-- [start:service]
// --8<-- [start:handler]
use ruststream_rumqttc::prelude::*;

#[subscriber(MqttFilter::new("devices/+/telemetry").shared("workers"))]
async fn record(telemetry: &Telemetry) -> HandlerOutcome {
    tracing::info!(telemetry.device, telemetry.temperature, "reading recorded");
    HandlerOutcome::ack()
}
// --8<-- [end:handler]

// --8<-- [start:app]
#[ruststream::app]
fn app() -> impl App {
    RustStream::new(AppInfo::new("telemetry", "0.1.0")).with_broker(
        MqttBroker::new("mqtt://localhost:1883", "telemetry-svc"),
        |b| {
            // A filter matches many topics and names none of them, so the mount site says where
            // a retry copy is published.
            b.include(record)
                .out_retry(Publish::default())
                .to("devices/retry/telemetry");
        },
    )
}
// --8<-- [end:app]
// --8<-- [end:service]

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
struct Telemetry {
    device: String,
    temperature: f64,
}

#[cfg(test)]
mod tests {
    use std::error::Error;

    use ruststream::testing::TestApp;
    use ruststream_rumqttc::prelude::*;

    use super::{Telemetry, app};

    #[tokio::test]
    async fn a_reading_from_any_device_is_recorded() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;
        let reading = Telemetry {
            device: "dev42".to_owned(),
            temperature: 21.5,
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
        Ok(())
    }
}
