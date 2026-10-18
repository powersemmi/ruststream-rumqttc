//! The service of `examples/mqtt_retained.rs` written without `#[subscriber]`: the handler implements `Handle` and reads
//! its slot from `Outs`, and the mount site is the same.
//!
//! ```text
//! cargo test --example manual_mqtt_retained --features testing
//! cargo run --example manual_mqtt_retained -- run
//! ```

use std::process::ExitCode;

use ruststream::runtime::PublishError;
use ruststream::runtime::cli::run_main;
use ruststream::schemars::JsonSchema;
// The error type is named here because this example handles it, in the hook's return type.
use ruststream_rumqttc::MqttError;
use ruststream_rumqttc::prelude::*;
use serde::{Deserialize, Serialize};

// --8<-- [start:state]
/// A device state announcement. An MQTT state is bytes on the wire rather than an encoded
/// model, so the type carries its own bytes and no codec runs on them; the name template turns
/// the device id into a setter the call fills in.
#[derive(Outgoing, Serialized)]
#[outgoing(name = "devices/{device}/state")]
struct DeviceState(Vec<u8>);

/// The service's own status, announced once it is connected.
#[derive(Outgoing, Serialized)]
#[outgoing(name = "services/state-tracker/status")]
struct ServiceStatus(Vec<u8>);
// --8<-- [end:state]

/// A reading. The type names no topic: each device publishes to its own.
#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing, JsonSchema)]
#[schemars(crate = "ruststream::schemars")]
struct Telemetry {
    device: String,
    temperature: f64,
}

/// The slot this handler announces through.
#[derive(OutSlot)]
#[publishes(DeviceState)]
struct States;

// --8<-- [start:stepped_handler]
/// A reading is one of many; the state it puts the device in is the one a service joining later
/// must see. So the handler publishes the state retained, and only the state: the step belongs
/// to this message, not to the publisher.
///
/// Naming a per-message setting is what ties a body to a broker, and its bound says so: the
/// options type is this crate's, and so is the prelude the file imports.
struct AnnounceState;

impl<Entry> Handle<Telemetry, (), Outs<(Entry,)>> for AnnounceState
where
    Entry: OutEntry<States, Wire: Publisher<Options = MqttPublishOptions>>,
{
    async fn handle(
        &self,
        telemetry: &Telemetry,
        outs: &Outs<(Entry,)>,
        _ctx: &mut Context<'_>,
    ) -> Result<(), HandlerOutcome> {
        let state = if telemetry.temperature > 30.0 {
            "hot"
        } else {
            "ok"
        };
        if let Err(err) = outs
            .get(States)
            .message(&DeviceState(state.as_bytes().to_vec()))
            .retain(true)
            .to()
            .device(&telemetry.device)
            .publish()
            .await
        {
            tracing::warn!(telemetry.device, %err, "state not announced");
            return Err(HandlerOutcome::retry());
        }
        Ok(())
    }
}

fn fleet() -> MqttFilter {
    MqttFilter::new("devices/+/telemetry").qos(Qos::AtLeastOnce)
}
// --8<-- [end:stepped_handler]

// --8<-- [start:app]
fn app() -> impl App {
    service(MqttBroker::new("mqtt://localhost:1883", "state-tracker"))
}

fn service(broker: MqttBroker) -> RustStream {
    RustStream::new(AppInfo::new("state-tracker", "0.1.0")).with_broker(broker, |b| {
        // Every publish of this policy is retained, so a dashboard that starts later still
        // reads the status.
        b.after_startup(
            Publish::default().qos(Qos::AtLeastOnce).retain(true),
            async move |publisher| -> Result<(), PublishError<MqttError>> {
                publisher
                    .message(&ServiceStatus(b"online".to_vec()))
                    .publish()
                    .await
            },
        );
        // The mount site fixes what every publish through the slot takes; the handler's step is
        // what one message changes.
        b.include(
            subscriber("devices/+/telemetry", AnnounceState)
                .map_source(|_| fleet())
                .build(),
        )
        .out(States, Publish::default().qos(Qos::AtLeastOnce))
        .out_retry(Publish::default())
        .to("devices/retry/telemetry")
        .build();
    })
}
// --8<-- [end:app]

// What `#[ruststream::app]` writes: the command line on a multi-threaded Tokio runtime.
fn main() -> ExitCode {
    run_main(app)
}

#[cfg(test)]
mod tests {
    use std::error::Error;
    use std::time::Duration;

    use ruststream::runtime::PublishError;
    use ruststream::testing::TestApp;
    use ruststream_rumqttc::MqttError;
    use ruststream_rumqttc::prelude::*;

    use super::{DeviceState, ServiceStatus, States, Telemetry, app, service};

    /// The stand's URL, or `None` without one; `RUSTSTREAM_REQUIRE_LIVE` turns the skip into a
    /// failure.
    fn live_url() -> Option<String> {
        match std::env::var("MQTT_TEST_URL") {
            Ok(url) if !url.is_empty() => Some(url),
            _ => {
                assert!(
                    std::env::var_os("RUSTSTREAM_REQUIRE_LIVE").is_none(),
                    "RUSTSTREAM_REQUIRE_LIVE is set, but MQTT_TEST_URL is missing or empty"
                );
                None
            }
        }
    }

    fn hot() -> Telemetry {
        Telemetry {
            device: "dev42".to_owned(),
            temperature: 31.5,
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_reading_publishes_the_device_state_retained() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;
        tb.broker::<MqttBroker>()
            .message(&hot())
            .to("devices/dev42/telemetry")
            .publish()
            .await?;

        tb.out::<States>()
            .assert_called_once()
            .with_raw(b"hot")
            .with_options(&MqttPublishOptions::default().retain(true));
        tb.broker::<MqttBroker>()
            .published::<()>("devices/dev42/state")
            .assert_called_once()
            .with_raw(b"hot");
        tb.shutdown().await?;
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn the_service_announces_its_status_at_startup() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;
        tb.settle().await?;

        tb.broker::<MqttBroker>()
            .published::<()>("services/state-tracker/status")
            .assert_called_once()
            .with_raw(b"online");
        tb.shutdown().await?;
        Ok(())
    }

    /// A payload read as the bytes it is.
    #[derive(Deserialized)]
    struct Raw<'a>(&'a [u8]);

    #[subscriber(MqttTopic::new("services/state-tracker/status"))]
    async fn watch_status(status: &Raw<'_>) {
        tracing::info!(bytes = status.0.len(), "status");
    }

    #[subscriber(MqttTopic::new("devices/dev42/state"))]
    async fn watch_state(state: &Raw<'_>) {
        tracing::info!(bytes = state.0.len(), "state");
    }

    /// A dashboard that subscribes after the announcements were made.
    fn dashboard(url: &str) -> RustStream {
        RustStream::new(AppInfo::new("dashboard", "0.1.0")).with_broker(
            MqttBroker::new(url, format!("dashboard-{}", std::process::id())),
            |b| {
                b.include(watch_status);
                b.include(watch_state);
            },
        )
    }

    /// Clears what an earlier run left on the two topics: an empty retained payload removes the
    /// retained message.
    fn clearing(url: &str) -> RustStream {
        RustStream::new(AppInfo::new("clearing", "0.1.0")).with_broker(
            MqttBroker::new(url, format!("clearing-{}", std::process::id())),
            |b| {
                b.after_startup(
                    Publish::default().retain(true),
                    async move |publisher| -> Result<(), PublishError<MqttError>> {
                        publisher
                            .message(&ServiceStatus(Vec::new()))
                            .publish()
                            .await?;
                        publisher
                            .message(&DeviceState(Vec::new()))
                            .to()
                            .device("dev42")
                            .publish()
                            .await
                    },
                );
            },
        )
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_dashboard_that_starts_later_reads_the_retained_states() -> Result<(), Box<dyn Error>>
    {
        let Some(url) = live_url() else {
            return Ok(());
        };
        TestApp::start_live(clearing(&url))
            .await?
            .shutdown()
            .await?;

        let id = format!("state-tracker-{}", std::process::id());
        let tb = TestApp::start_live(service(MqttBroker::new(&url, id))).await?;
        tb.broker::<MqttBroker>()
            .message(&hot())
            .to("devices/dev42/telemetry")
            .publish()
            .await?;
        tb.shutdown().await?;

        let tb = TestApp::start_live(dashboard(&url)).await?;
        // Nothing is published here: the retained messages arrive with the subscriptions, so the
        // harness lets a moment of real time pass for them.
        tb.advance(Duration::from_secs(1)).await?;
        tb.broker::<MqttBroker>()
            .subscriber("services/state-tracker/status")
            .assert_called_once()
            .with_raw(b"online");
        tb.broker::<MqttBroker>()
            .subscriber("devices/dev42/state")
            .assert_called_once()
            .with_raw(b"hot");
        tb.shutdown().await?;
        Ok(())
    }
}
