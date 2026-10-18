//! A persistent session across a planned restart of the service, against a real MQTT broker,
//! gated behind `MQTT_TEST_URL`.
//!
//! The subject is what the server keeps of the session once the service stops cleanly, which only
//! a server can show: `just test-brokers` (mosquitto).

#![cfg(feature = "testing")]

use std::time::Duration;

use ruststream::ConnectedBroker;
use ruststream::testing::TestApp;
use ruststream_rumqttc::prelude::*;
use serde::{Deserialize, Serialize};

/// The live broker URL, or `None` when there is no stand; `RUSTSTREAM_REQUIRE_LIVE` turns the skip
/// into a failure, so a job that means to run live cannot pass without running.
fn test_url() -> Option<String> {
    match std::env::var("MQTT_TEST_URL") {
        Ok(url) if !url.is_empty() => Some(url),
        _ => {
            assert!(
                std::env::var_os("RUSTSTREAM_REQUIRE_LIVE").is_none(),
                "RUSTSTREAM_REQUIRE_LIVE is set, so the live session suite must run, \
                 but MQTT_TEST_URL is missing or empty"
            );
            eprintln!("MQTT_TEST_URL is not set; skipping the live session test");
            None
        }
    }
}

/// The topic of this run, so a session another run left on the stand never matches it.
fn readings() -> String {
    format!("session/readings/{}", std::process::id())
}

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
struct Reading {
    celsius: f64,
}

#[subscriber(MqttTopic::new(readings()).qos(Qos::AtLeastOnce))]
async fn record(reading: &Reading) {
    let _ = reading.celsius;
}

/// The service: one subscription on a session the server keeps for a day between connections.
fn app(url: &str) -> RustStream {
    RustStream::new(AppInfo::new("session-restart", "0.1.0")).with_broker(
        MqttBroker::new(url, format!("session-restart-{}", std::process::id()))
            .clean_start(false)
            .session_expiry(Duration::from_hours(24)),
        |b| {
            b.include(record);
        },
    )
}

/// A service on a persistent session stops cleanly, a reading is published while it is down, and
/// the service that starts again under the same client id receives it: the session it left kept
/// its subscription.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_clean_shutdown_keeps_the_subscriptions_of_a_persistent_session() {
    let Some(url) = test_url() else { return };

    let tb = TestApp::start_live(app(&url))
        .await
        .expect("the service starts");
    tb.shutdown().await.expect("the service stops");

    let sender = MqttBroker::new(&url, format!("session-sender-{}", std::process::id()))
        .connect()
        .await
        .expect("the sender connects");
    sender
        .publisher()
        .message(&Reading { celsius: 30.5 })
        .to(readings())
        .publish()
        .await
        .expect("the reading is published while the service is down");
    sender.shutdown().await.expect("the sender stops");

    let tb = TestApp::start_live(app(&url))
        .await
        .expect("the service starts again");
    // The server hands a resumed session what it queued before anything published after the
    // restart, and the harness returns once the service has handled as many readings as it saw
    // published, so the first one handled is the one that waited for the service.
    tb.broker::<MqttBroker>()
        .message(&Reading { celsius: 31.0 })
        .to(readings())
        .publish()
        .await
        .expect("a reading after the restart reaches the service");

    let received = tb
        .broker::<MqttBroker>()
        .subscriber(&readings())
        .received::<Reading>();
    assert_eq!(
        received.first(),
        Some(&Reading { celsius: 30.5 }),
        "the reading published while the service was down was lost: {received:?}"
    );
    tb.shutdown().await.expect("the service stops");
}
