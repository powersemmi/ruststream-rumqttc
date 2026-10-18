//! The service of `examples/mqtt_settlement_retry.rs` written without `#[subscriber]`: the
//! handler implements `Handle`, and the broker keeps the same persistent session.
//!
//! ```text
//! cargo test --example manual_mqtt_settlement_retry --features testing
//! cargo run --example manual_mqtt_settlement_retry -- run
//! ```

use std::future::{Future, ready};
use std::process::ExitCode;
use std::time::Duration;

use ruststream::runtime::cli::run_main;
use ruststream::schemars::JsonSchema;
use ruststream_rumqttc::prelude::*;
use serde::{Deserialize, Serialize};

/// A firmware image offered to one device.
#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing, JsonSchema)]
#[schemars(crate = "ruststream::schemars")]
#[outgoing(name = "devices/dev42/firmware")]
struct Firmware {
    version: String,
    needs_reboot: bool,
}

// --8<-- [start:handler]
/// An update that needs a reboot waits for the next session: the handler leaves it
/// unacknowledged, and the server hands it back when the device agent reconnects.
struct Install;

impl Handle<Firmware> for Install {
    fn handle(
        &self,
        update: &Firmware,
        _outs: &(),
        _ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<(), HandlerOutcome>> {
        if update.needs_reboot {
            tracing::info!(update.version, "update left for the next session");
            return ready(Err(HandlerOutcome::retry()));
        }
        tracing::info!(update.version, "update installed");
        ready(Ok(()))
    }
}

fn firmware() -> MqttTopic {
    MqttTopic::new("devices/dev42/firmware").qos(Qos::AtLeastOnce)
}
// --8<-- [end:handler]

// --8<-- [start:app]
fn app() -> impl App {
    service(MqttBroker::new(
        "mqtt://localhost:1883",
        "device-agent-dev42",
    ))
}

/// The session outlives the connection by a day, so what the handler left unacknowledged comes
/// back when the agent connects again within it, under the same client id.
fn service(broker: MqttBroker) -> RustStream {
    RustStream::new(AppInfo::new("device-agent", "0.1.0")).with_broker(
        broker
            .clean_start(false)
            .session_expiry(Duration::from_hours(24)),
        |b| {
            b.include(
                subscriber("devices/dev42/firmware", Install)
                    .map_source(|_| firmware())
                    .build(),
            );
        },
    )
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

    use ruststream::testing::TestApp;
    use ruststream_rumqttc::prelude::*;

    use super::{Firmware, app, service};

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

    fn major_update() -> Firmware {
        Firmware {
            version: "2.0.0".to_owned(),
            needs_reboot: true,
        }
    }

    /// Within the connection nothing asks the server for the message again.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_retried_update_does_not_come_back_within_the_connection()
    -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;
        tb.broker::<MqttBroker>()
            .message(&major_update())
            .publish()
            .await?;
        tb.settle().await?;

        tb.broker::<MqttBroker>()
            .subscriber("devices/dev42/firmware")
            .assert_called_once()
            .settled(HandlerOutcome::retry());
        tb.shutdown().await?;
        Ok(())
    }

    /// The server keeps the unacknowledged delivery in the session and sends it again when the
    /// same client id resumes it.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_retried_update_comes_back_when_the_session_resumes() -> Result<(), Box<dyn Error>> {
        let Some(url) = live_url() else {
            return Ok(());
        };
        let client_id = format!("device-agent-{}", std::process::id());

        let tb = TestApp::start_live(service(MqttBroker::new(&url, &client_id))).await?;
        tb.broker::<MqttBroker>()
            .message(&major_update())
            .publish()
            .await?;
        tb.broker::<MqttBroker>()
            .subscriber("devices/dev42/firmware")
            .assert_called_once()
            .settled(HandlerOutcome::retry());
        tb.shutdown().await?;

        let tb = TestApp::start_live(service(MqttBroker::new(&url, &client_id))).await?;
        // Nothing is published here: the message arrives from the session, so the harness lets
        // a moment of real time pass for it.
        tb.advance(Duration::from_secs(1)).await?;
        tb.broker::<MqttBroker>()
            .subscriber("devices/dev42/firmware")
            .assert_called_once()
            .with(&major_update());
        tb.shutdown().await?;

        // A clean start discards the session, so the update this test left is not kept.
        let cleanup = RustStream::new(AppInfo::new("device-agent", "0.1.0"))
            .register_broker(MqttBroker::new(&url, &client_id));
        TestApp::start_live(cleanup).await?.shutdown().await?;
        Ok(())
    }
}
