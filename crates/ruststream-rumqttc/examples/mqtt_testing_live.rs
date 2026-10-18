//! One test body, two modes: in process under `TestApp::start`, and against a running MQTT server
//! under `TestApp::start_live`. The live test skips without `MQTT_TEST_URL`, and
//! `RUSTSTREAM_REQUIRE_LIVE=1` turns that skip into a failure.
//!
//! ```text
//! cargo test --example mqtt_testing_live --features testing
//! just brokers-up
//! MQTT_TEST_URL=mqtt://127.0.0.1:1883 RUSTSTREAM_REQUIRE_LIVE=1 \
//!     cargo test --example mqtt_testing_live --features testing
//! ```

// A plain build compiles the service; the tests that use it exist only in the test build.
#![cfg_attr(not(test), allow(dead_code, unused_imports))]

use serde::{Deserialize, Serialize};

use ruststream_rumqttc::prelude::*;

#[cfg(test)]
#[path = "../tests/live_broker/mod.rs"]
mod live;

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
#[outgoing(name = "testing-live/telemetry")]
struct Telemetry {
    device: String,
    temperature: f64,
}

#[subscriber("testing-live/telemetry")]
async fn record(telemetry: &Telemetry) -> HandlerOutcome {
    tracing::info!(telemetry.device, telemetry.temperature, "reading recorded");
    HandlerOutcome::ack()
}

/// The service on the server at `url`, connected under `client_id`.
fn app(url: &str, client_id: &str) -> impl App<State = ()> {
    RustStream::new(AppInfo::new("telemetry", "0.1.0")).with_broker(
        MqttBroker::new(url, client_id),
        |b| {
            b.include(record);
        },
    )
}

#[ruststream::app]
fn service() -> impl App<State = ()> {
    app("mqtt://localhost:1883", "telemetry-svc")
}

#[cfg(test)]
mod tests {
    use std::error::Error;

    use ruststream::testing::TestApp;
    use ruststream_rumqttc::prelude::*;

    use super::{Telemetry, app, live};

    // --8<-- [start:both]
    /// The body both modes run; only the start call differs.
    async fn records_a_reading(tb: TestApp<()>) -> Result<(), Box<dyn Error>> {
        let reading = Telemetry {
            device: "dev42".to_owned(),
            temperature: 21.5,
        };
        tb.broker::<MqttBroker>()
            .message(&reading)
            .publish()
            .await?;

        tb.broker::<MqttBroker>()
            .subscriber("testing-live/telemetry")
            .assert_called_once()
            .with(&reading)
            .settled(HandlerOutcome::ack());
        tb.shutdown().await?;
        Ok(())
    }

    #[tokio::test]
    async fn in_process() -> Result<(), Box<dyn Error>> {
        records_a_reading(TestApp::start(app("mqtt://localhost:1883", "telemetry-svc")).await?)
            .await
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn against_the_stand() -> Result<(), Box<dyn Error>> {
        let Some(url) = live::url("MQTT_TEST_URL") else {
            return Ok(());
        };
        // A client id of this run's own: a second connection under one id takes the session over.
        let tb = TestApp::start_live(app(&url, &live::client_id("telemetry"))).await?;
        records_a_reading(tb).await
    }
    // --8<-- [end:both]
}
