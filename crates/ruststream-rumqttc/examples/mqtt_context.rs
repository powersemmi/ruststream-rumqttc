//! What an MQTT delivery carries besides its payload and headers: the topic it was published to,
//! read as a handler parameter with `Ctx<DeliveryTopic>`. The scenario is a test.
//!
//! ```text
//! cargo test --example mqtt_context --features testing
//! cargo run --example mqtt_context --features testing -- run
//! ```

use ruststream_rumqttc::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
struct Alarm {
    code: u32,
}

/// An alarm, with the device that raised it.
#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
#[outgoing(name = "incidents")]
struct Incident {
    device: String,
    code: u32,
}

// --8<-- [start:handler]
/// The alarm itself names no device: its topic does, at the level the filter's `+` matched.
#[subscriber(MqttFilter::new("devices/+/alarms"), reply)]
async fn open_incident(alarm: &Alarm, Ctx(topic): Ctx<DeliveryTopic>) -> Incident {
    let device = topic.split('/').nth(1).unwrap_or_default().to_owned();
    tracing::info!(%topic, alarm.code, "incident opened");
    Incident {
        device,
        code: alarm.code,
    }
}
// --8<-- [end:handler]

#[ruststream::app]
fn app() -> impl App {
    RustStream::new(AppInfo::new("incidents", "0.1.0")).with_broker(
        MqttBroker::new("mqtt://localhost:1883", "incidents-svc"),
        |b| {
            b.include(open_incident)
                .out_reply(Publish::default())
                .out_retry(Publish::default())
                .to("devices/retry/alarms");
        },
    )
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
