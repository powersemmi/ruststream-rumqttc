//! Conformance: the routing suite against the in-process transport, and the lifecycle check
//! against a real broker (gated behind `MQTT_TEST_URL`).
//!
//! Start one with `just brokers-up` (mosquitto), then:
//! `MQTT_TEST_URL=mqtt://127.0.0.1:1883 cargo test --all-features`.

#![cfg(feature = "testing")]

use ruststream::conformance::harness;
use ruststream_rumqttc::testing::MqttTestBroker;
use ruststream_rumqttc::{MqttBroker, MqttTopic};

fn test_url() -> Option<String> {
    match std::env::var("MQTT_TEST_URL") {
        Ok(url) if !url.is_empty() => Some(url),
        _ => {
            eprintln!("MQTT_TEST_URL is not set; skipping the live conformance check");
            None
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn mqtt_test_broker_passes_conformance_suite() {
    harness::run_suite(MqttTestBroker::new).await;
}

// `make_source` / `make_publisher` must stay closures: their bounds are higher-ranked
// (`Fn(&str) -> _` / `Fn(&B) -> _`), so a bare method path - which binds one concrete lifetime -
// would not type-check.
#[allow(clippy::redundant_closure, clippy::redundant_closure_for_method_calls)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn mqtt_broker_passes_lifecycle() {
    let Some(url) = test_url() else { return };
    harness::lifecycle(
        || MqttBroker::new(url.clone(), format!("lifecycle-{}", std::process::id())),
        |name| MqttTopic::new(name),
        |connected| connected.publisher(),
    )
    .await;
}
