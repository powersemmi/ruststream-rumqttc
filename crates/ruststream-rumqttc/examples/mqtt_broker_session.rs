//! A persistent session: the server keeps the subscriptions and the unacknowledged messages of a
//! service that disconnects, and hands them over when it connects again. The session lives on the
//! server, so its test runs against the stand and skips without it.
//!
//! ```text
//! cargo test --example mqtt_broker_session --features testing
//! just brokers-up
//! MQTT_TEST_URL=mqtt://127.0.0.1:1883 cargo test --example mqtt_broker_session --features testing
//! ```

// A plain build compiles the service; the tests that use it exist only in the test build.
#![cfg_attr(not(test), allow(dead_code, unused_imports))]

use std::time::Duration;

use serde::{Deserialize, Serialize};

use ruststream_rumqttc::prelude::*;

#[cfg(test)]
#[path = "../tests/live_broker/mod.rs"]
mod live;

#[subscriber("broker-session/telemetry")]
async fn record(telemetry: &Telemetry) -> HandlerOutcome {
    tracing::info!(telemetry.device, telemetry.temperature, "reading recorded");
    HandlerOutcome::ack()
}

fn service(broker: MqttBroker) -> impl App<State = ()> {
    RustStream::new(AppInfo::new("telemetry", "0.1.0")).with_broker(broker, |b| {
        b.include(record);
    })
}

// --8<-- [start:broker]
/// The server at `url` keeps this client's session for a day after the service disconnects.
fn broker(url: &str, client_id: &str) -> MqttBroker {
    MqttBroker::new(url, client_id)
        .clean_start(false)
        .session_expiry(Duration::from_hours(24))
}

#[ruststream::app]
fn app() -> impl App<State = ()> {
    service(broker("mqtt://localhost:1883", "telemetry-svc"))
}
// --8<-- [end:broker]

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
#[outgoing(name = "broker-session/telemetry")]
struct Telemetry {
    device: String,
    temperature: f64,
}

#[cfg(test)]
mod tests {
    use std::error::Error;

    use ruststream::testing::TestApp;
    use ruststream_rumqttc::prelude::*;

    use super::{Telemetry, app, broker, live, service};

    fn reading(temperature: f64) -> Telemetry {
        Telemetry {
            device: "dev42".to_owned(),
            temperature,
        }
    }

    #[tokio::test]
    async fn the_service_with_a_persistent_session_runs_in_process() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;
        tb.broker::<MqttBroker>()
            .message(&reading(21.5))
            .publish()
            .await?;

        tb.broker::<MqttBroker>()
            .subscriber("broker-session/telemetry")
            .assert_called_once()
            .settled(HandlerOutcome::ack());
        Ok(())
    }

    /// Publishes a reading from a client of its own while the service is not connected.
    async fn publish_while_down(url: &str) -> Result<(), Box<dyn Error + Send + Sync>> {
        let sender = RustStream::new(AppInfo::new("sender", "0.1.0")).register_broker(
            MqttBroker::new(url, live::client_id("broker-session-sender")),
        );
        let sender = TestApp::start_live(sender).await?;
        sender
            .broker::<MqttBroker>()
            .message(&reading(30.5))
            .publish()
            .await?;
        sender.shutdown().await?;
        Ok(())
    }

    /// Starts the service again under `id` and checks that the server sends the reading it kept
    /// before the one published now, then ends the session the test left on the server.
    async fn resume_and_receive_the_kept_reading(
        url: &str,
        id: &str,
    ) -> Result<(), Box<dyn Error + Send + Sync>> {
        let tb = TestApp::start_live(service(broker(url, id))).await?;
        tb.broker::<MqttBroker>()
            .message(&reading(21.5))
            .publish()
            .await?;
        let received = tb
            .broker::<MqttBroker>()
            .subscriber("broker-session/telemetry")
            .received::<Telemetry>();
        assert_eq!(received.first(), Some(&reading(30.5)), "{received:?}");
        tb.shutdown().await?;

        // A clean start under the same id ends the session.
        TestApp::start_live(service(MqttBroker::new(url, id)))
            .await?
            .shutdown()
            .await?;
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_reading_published_while_the_service_was_stopped_arrives_on_restart()
    -> Result<(), Box<dyn Error + Send + Sync>> {
        let Some(url) = live::url("MQTT_TEST_URL") else {
            return Ok(());
        };
        let id = live::client_id("broker-session-restart");

        // The first run opens the session and its subscription, then stops cleanly: the
        // subscription stays in the session.
        TestApp::start_live(service(broker(&url, &id)))
            .await?
            .shutdown()
            .await?;

        publish_while_down(&url).await?;
        resume_and_receive_the_kept_reading(&url, &id).await
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_reading_published_while_the_service_was_down_arrives_on_return()
    -> Result<(), Box<dyn Error + Send + Sync>> {
        let Some(url) = live::url("MQTT_TEST_URL") else {
            return Ok(());
        };
        let id = live::client_id("broker-session");

        // The first run opens the session and its subscription, then the process dies.
        let crash_url = url.clone();
        let crash_id = id.clone();
        live::crash(move || service(broker(&crash_url, &crash_id)))?;

        publish_while_down(&url).await?;
        resume_and_receive_the_kept_reading(&url, &id).await
    }
}
