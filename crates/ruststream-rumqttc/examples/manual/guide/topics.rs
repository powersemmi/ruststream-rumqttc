//! The service of the Topics guide written without the macros: each handler implements `Handle`,
//! and each descriptor joins its chain through `map_source`.
//!
//! ```text
//! cargo test --example manual_guide_topics --features testing
//! cargo run --example manual_guide_topics --features testing -- run
//! ```

use std::future::{Future, ready};
use std::process::ExitCode;

use ruststream::runtime::cli::run_main;
use ruststream::schemars::JsonSchema;
use ruststream_rumqttc::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing, JsonSchema)]
#[schemars(crate = "ruststream::schemars")]
struct Telemetry {
    temperature: f64,
}

// --8<-- [start:store]
/// Every replica of the service joins the share group `ingest`, so each reading is stored once.
/// A body typed over `MqttContext` reads the topic the reading arrived on.
struct Store;

impl Handle<Telemetry, (), (), MqttContext> for Store {
    fn handle(
        &self,
        telemetry: &Telemetry,
        _outs: &(),
        ctx: &mut Context<'_, MqttContext>,
    ) -> impl Future<Output = Result<(), HandlerOutcome>> {
        let topic = ctx.context(DeliveryTopic);
        tracing::info!(%topic, telemetry.temperature, "reading stored");
        ready(Ok(()))
    }
}

fn fleet() -> MqttFilter {
    MqttFilter::new("devices/+/telemetry")
        .qos(Qos::AtLeastOnce)
        .shared("ingest")
}
// --8<-- [end:store]

// --8<-- [start:trace]
/// A payload taken as the bytes it is, because one device sends messages of several kinds.
#[derive(Deserialized)]
struct Raw<'a>(&'a [u8]);

/// Logs every message of the device under investigation, whatever its kind.
struct Trace;

impl Handle<Raw<'_>, (), (), MqttContext> for Trace {
    fn handle(
        &self,
        message: &Raw<'_>,
        _outs: &(),
        ctx: &mut Context<'_, MqttContext>,
    ) -> impl Future<Output = Result<(), HandlerOutcome>> {
        let topic = ctx.context(DeliveryTopic);
        tracing::debug!(%topic, bytes = message.0.len(), "dev42 traced");
        ready(Ok(()))
    }
}

fn one_device() -> MqttFilter {
    MqttFilter::new("devices/dev42/#")
}
// --8<-- [end:trace]

// --8<-- [start:firmware]
#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing, JsonSchema)]
#[schemars(crate = "ruststream::schemars")]
#[outgoing(name = "fleet/firmware")]
struct FirmwareRelease {
    version: String,
}

/// Mounted under a bare name, which is one topic read at the default quality of service.
struct ScheduleRollout;

impl Handle<FirmwareRelease> for ScheduleRollout {
    fn handle(
        &self,
        release: &FirmwareRelease,
        _outs: &(),
        _ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<(), HandlerOutcome>> {
        tracing::info!(release.version, "rollout scheduled");
        ready(Ok(()))
    }
}
// --8<-- [end:firmware]

// --8<-- [start:app]
fn app() -> impl App<State = ()> {
    RustStream::new(AppInfo::new("fleet", "0.1.0")).with_broker(
        MqttBroker::new("mqtt://localhost:1883", "fleet-svc"),
        |b| {
            // A filter names no topic of its own, so each one names where a retry copy goes:
            // a topic it matches and no other filter of the service does.
            b.include(
                subscriber("devices/+/telemetry", Store)
                    .map_source(|_| fleet())
                    .build(),
            )
            .out_retry(Publish::default())
            .to("devices/retry/telemetry");
            b.include(
                subscriber("devices/dev42/#", Trace)
                    .map_source(|_| one_device())
                    .build(),
            )
            .out_retry(Publish::default())
            .to("devices/dev42/retry");
            b.include(subscriber("fleet/firmware", ScheduleRollout).build());
        },
    )
}

fn main() -> ExitCode {
    run_main(app)
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
