//! The prelude: one glob brings the core's prelude, the broker, its descriptors and their quality
//! of service, the publish policy under the name every broker crate uses, and the context key.
//!
//! ```text
//! cargo test --example mqtt_broker_prelude --features testing
//! ```

use serde::{Deserialize, Serialize};

// --8<-- [start:app]
use ruststream_rumqttc::prelude::*;

#[subscriber(MqttFilter::new("devices/+/telemetry").qos(Qos::AtLeastOnce))]
async fn check(
    telemetry: &Telemetry,
    Ctx(topic): Ctx<DeliveryTopic>,
    Out(alerts): Out<impl Publisher>,
) -> HandlerOutcome {
    if telemetry.temperature <= 30.0 {
        return HandlerOutcome::ack();
    }
    if alerts.message(&Alert { topic }).publish().await.is_err() {
        return HandlerOutcome::retry();
    }
    HandlerOutcome::ack()
}

#[ruststream::app]
fn app() -> impl App {
    RustStream::new(AppInfo::new("telemetry", "0.1.0")).with_broker(
        MqttBroker::new("mqtt://localhost:1883", "telemetry-svc"),
        |b| {
            b.include(check)
                .out(DefaultSlot, Publish::default().qos(Qos::ExactlyOnce))
                .out_retry(Publish::default())
                .to("devices/retry/telemetry")
                .build();
        },
    )
}
// --8<-- [end:app]

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
struct Telemetry {
    temperature: f64,
}

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
#[outgoing(name = "alerts")]
struct Alert {
    topic: String,
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
