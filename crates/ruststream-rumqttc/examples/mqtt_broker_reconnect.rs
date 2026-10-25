//! Reconnect: the connection task notices a dead connection through keep-alive, dials again as the
//! broker's reconnect mode says, and resumes the session. The keep-alive floor is checked in
//! process; a lost connection needs a server, so that test runs against the stand and skips
//! without it.
//!
//! ```text
//! cargo test --example mqtt_broker_reconnect --features testing
//! just brokers-up
//! MQTT_TEST_URL=mqtt://127.0.0.1:1883 cargo test --example mqtt_broker_reconnect --features testing
//! ```

// A plain build compiles the service; the tests that use it exist only in the test build.
#![cfg_attr(not(test), allow(dead_code, unused_imports))]

use std::num::NonZeroU32;
use std::time::Duration;

use ruststream::runtime::SubscriptionEnd;
use serde::{Deserialize, Serialize};

use ruststream_rumqttc::prelude::*;
use ruststream_rumqttc::{MqttError, Reconnected, Reconnector, Recovery};

#[cfg(test)]
#[path = "../tests/live_broker/mod.rs"]
mod live;

#[subscriber("broker-reconnect/telemetry")]
async fn record(telemetry: &Telemetry) -> HandlerOutcome {
    tracing::info!(telemetry.device, telemetry.temperature, "reading recorded");
    HandlerOutcome::ack()
}

fn service(broker: MqttBroker) -> impl App<State = ()> {
    RustStream::new(AppInfo::new("telemetry", "0.1.0"))
        .on_subscription_end(SubscriptionEnd::FailFast)
        .with_broker(broker, |b| {
            b.include(record);
        })
}

// --8<-- [start:broker]
/// About ten minutes of dials: pauses from 100 ms up to 5 s, then every subscription ends.
const ATTEMPTS: NonZeroU32 = NonZeroU32::new(120).unwrap();

/// A connection silent for 15 seconds is probed, and one that stays silent is dropped and dialed
/// again. The session outlives the gap by an hour, so what arrives meanwhile is kept.
fn broker(url: &str, client_id: &str) -> MqttBroker {
    MqttBroker::new(url, client_id)
        .keep_alive(Duration::from_secs(15))
        .clean_start(false)
        .session_expiry(Duration::from_hours(1))
        .reconnect(Reconnect::automatic(
            AutoReconnect::new().attempts(ATTEMPTS),
        ))
}

#[ruststream::app]
fn app() -> impl App<State = ()> {
    service(broker("mqtt://localhost:1883", "telemetry-svc"))
}
// --8<-- [end:broker]

// --8<-- [start:custom]
/// Logs in with a fresh password on every dial, and subscribes again what a lost session took.
struct FreshLogin;

/// The password as the secret store holds it now, which a rotation may have changed.
fn current_password() -> String {
    std::env::var("MQTT_PASSWORD").unwrap_or_default()
}

impl Reconnector for FreshLogin {
    async fn reconnect(&self, recovery: &mut Recovery<'_>) -> Result<Reconnected, MqttError> {
        tracing::warn!(reason = %recovery.lost(), "mqtt connection lost");
        let mut failed = 0;
        let reconnected = loop {
            recovery.credentials("telemetry-svc", current_password());
            match recovery.connect().await {
                Ok(reconnected) => break reconnected,
                Err(err) if failed == 30 => return Err(err),
                Err(_) => failed += 1,
            }
            tokio::time::sleep(Duration::from_secs(2)).await;
        };
        if !reconnected.session_present() {
            recovery.restore_all().await?;
        }
        Ok(reconnected)
    }
}

fn custom_broker(url: &str, client_id: &str) -> MqttBroker {
    MqttBroker::new(url, client_id).reconnect(Reconnect::custom(FreshLogin))
}
// --8<-- [end:custom]

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
#[outgoing(name = "broker-reconnect/telemetry")]
struct Telemetry {
    device: String,
    temperature: f64,
}

#[cfg(test)]
mod tests {
    use std::error::Error;
    use std::time::Duration;

    use ruststream::testing::TestApp;
    use ruststream_rumqttc::prelude::*;

    use super::{Telemetry, app, broker, custom_broker, live, service};

    fn reading() -> Telemetry {
        Telemetry {
            device: "dev42".to_owned(),
            temperature: 21.5,
        }
    }

    #[tokio::test]
    async fn a_keep_alive_under_five_seconds_does_not_start() {
        let broker =
            broker("mqtt://localhost:1883", "telemetry-svc").keep_alive(Duration::from_secs(2));

        let refused = TestApp::start(service(broker)).await.err();
        let refused = refused.map(|err| err.to_string()).unwrap_or_default();
        assert!(refused.contains("at least 5 seconds"), "{refused}");
    }

    #[tokio::test]
    async fn the_service_runs_in_process() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;
        tb.broker::<MqttBroker>()
            .message(&reading())
            .publish()
            .await?;

        tb.broker::<MqttBroker>()
            .subscriber("broker-reconnect/telemetry")
            .assert_called_once()
            .settled(HandlerOutcome::ack());
        Ok(())
    }

    #[tokio::test]
    async fn the_service_with_its_own_reconnection_runs_in_process() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(service(custom_broker(
            "mqtt://localhost:1883",
            "telemetry-svc",
        )))
        .await?;
        tb.broker::<MqttBroker>()
            .message(&reading())
            .publish()
            .await?;

        tb.broker::<MqttBroker>()
            .subscriber("broker-reconnect/telemetry")
            .assert_called_once()
            .settled(HandlerOutcome::ack());
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn the_subscription_comes_back_after_the_connection_is_lost()
    -> Result<(), Box<dyn Error + Send + Sync>> {
        let Some(url) = live::url("MQTT_TEST_URL") else {
            return Ok(());
        };
        let id = live::client_id("broker-reconnect");
        let tb = TestApp::start_live(service(broker(&url, &id))).await?;

        // A second process under the same client id takes the session over, so the server closes
        // the service's connection; that process then dies and leaves the session to whoever
        // comes back for it.
        let twin_url = url.clone();
        let twin_id = id.clone();
        live::crash(move || service(broker(&twin_url, &twin_id)))?;

        // The publish leaves once the service has reconnected, and the resumed session delivers it.
        tb.broker::<MqttBroker>()
            .message(&reading())
            .publish()
            .await?;
        tb.broker::<MqttBroker>()
            .subscriber("broker-reconnect/telemetry")
            .assert_called_once()
            .with(&reading())
            .settled(HandlerOutcome::ack());
        tb.shutdown().await?;

        // A clean start under the same id ends the session the test left on the server.
        TestApp::start_live(service(MqttBroker::new(&url, &id)))
            .await?
            .shutdown()
            .await?;
        Ok(())
    }
}
