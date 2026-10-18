//! The service of the Topics guide: a shared subscription over the telemetry of every device, a
//! trace of every message of one device, and one exact topic for firmware releases.
//!
//! ```text
//! cargo test --example guide_topics --features testing
//! cargo run --example guide_topics --features testing -- run
//! ```

use ruststream_rumqttc::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
struct Telemetry {
    temperature: f64,
}

// --8<-- [start:store]
/// Every replica of the service joins the share group `ingest`, so each reading is stored once.
#[subscriber(MqttFilter::new("devices/+/telemetry").qos(Qos::AtLeastOnce).shared("ingest"))]
async fn store(telemetry: &Telemetry, Ctx(topic): Ctx<DeliveryTopic>) -> HandlerOutcome {
    tracing::info!(%topic, telemetry.temperature, "reading stored");
    HandlerOutcome::ack()
}
// --8<-- [end:store]

// --8<-- [start:trace]
/// A payload taken as the bytes it is, because one device sends messages of several kinds.
#[derive(Deserialized)]
struct Raw<'a>(&'a [u8]);

/// Logs every message of the device under investigation, whatever its kind.
#[subscriber(MqttFilter::new("devices/dev42/#"))]
async fn trace(message: &Raw<'_>, Ctx(topic): Ctx<DeliveryTopic>) -> HandlerOutcome {
    tracing::debug!(%topic, bytes = message.0.len(), "dev42 traced");
    HandlerOutcome::ack()
}
// --8<-- [end:trace]

// --8<-- [start:firmware]
#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
#[outgoing(name = "fleet/firmware")]
struct FirmwareRelease {
    version: String,
}

/// A bare string is one topic, read at the default quality of service.
#[subscriber("fleet/firmware")]
async fn schedule_rollout(release: &FirmwareRelease) -> HandlerOutcome {
    tracing::info!(release.version, "rollout scheduled");
    HandlerOutcome::ack()
}
// --8<-- [end:firmware]

// --8<-- [start:app]
#[ruststream::app]
fn app() -> impl App<State = ()> {
    RustStream::new(AppInfo::new("fleet", "0.1.0")).with_broker(
        MqttBroker::new("mqtt://localhost:1883", "fleet-svc"),
        |b| {
            // A filter names no topic of its own, so each one names where a retry copy goes:
            // a topic it matches and no other filter of the service does.
            b.include(store)
                .out_retry(Publish::default())
                .to("devices/retry/telemetry");
            b.include(trace)
                .out_retry(Publish::default())
                .to("devices/dev42/retry");
            b.include(schedule_rollout);
        },
    )
}
// --8<-- [end:app]

#[cfg(test)]
mod tests {
    use std::error::Error;

    use ruststream::testing::TestApp;
    use ruststream_rumqttc::prelude::*;

    use super::{FirmwareRelease, Telemetry, app};

    const STORE: &str = "devices/+/telemetry";
    const TRACE: &str = "devices/dev42/#";

    async fn publish_reading(tb: &TestApp<()>, topic: &str) -> Result<(), Box<dyn Error>> {
        tb.broker::<MqttBroker>()
            .message(&Telemetry { temperature: 21.5 })
            .to(topic)
            .publish()
            .await?;
        Ok(())
    }

    #[tokio::test]
    async fn overlapping_filters_each_read_a_reading_once() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;

        publish_reading(&tb, "devices/dev42/telemetry").await?;

        tb.broker::<MqttBroker>()
            .subscriber(STORE)
            .assert_called_once()
            .with(&Telemetry { temperature: 21.5 });
        tb.broker::<MqttBroker>()
            .subscriber(TRACE)
            .assert_called_once();
        tb.shutdown().await?;
        Ok(())
    }

    #[tokio::test]
    async fn another_device_reaches_the_store_only() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;

        publish_reading(&tb, "devices/dev7/telemetry").await?;

        tb.broker::<MqttBroker>()
            .subscriber(STORE)
            .assert_called_once();
        tb.broker::<MqttBroker>()
            .subscriber(TRACE)
            .assert_not_called();
        tb.shutdown().await?;
        Ok(())
    }

    #[tokio::test]
    async fn a_deeper_topic_reaches_the_multi_level_wildcard_only() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;

        publish_reading(&tb, "devices/dev42/telemetry/raw").await?;

        tb.broker::<MqttBroker>()
            .subscriber(STORE)
            .assert_not_called();
        tb.broker::<MqttBroker>()
            .subscriber(TRACE)
            .assert_called_once();
        tb.shutdown().await?;
        Ok(())
    }

    #[tokio::test]
    async fn a_firmware_release_reaches_its_topic() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;
        let release = FirmwareRelease {
            version: "2.1.0".to_owned(),
        };

        tb.broker::<MqttBroker>()
            .message(&release)
            .publish()
            .await?;

        tb.broker::<MqttBroker>()
            .subscriber("fleet/firmware")
            .assert_called_once()
            .with(&release)
            .settled(HandlerOutcome::ack());
        tb.shutdown().await?;
        Ok(())
    }
}
