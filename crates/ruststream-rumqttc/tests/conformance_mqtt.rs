//! Conformance: the routing suite against the in-process transport, and the lifecycle check
//! against a real broker (gated behind `MQTT_TEST_URL`).
//!
//! Start one with `just brokers-up` (mosquitto), then:
//! `MQTT_TEST_URL=mqtt://127.0.0.1:1883 cargo test --all-features`.

#![cfg(feature = "testing")]

use ruststream::Name;
use ruststream::conformance::{capabilities, harness};
use ruststream_rumqttc::testing::MqttTestBroker;
use ruststream_rumqttc::{MqttBroker, MqttTopic};

/// The live broker URL, or `None` when there is no stand to run against.
///
/// Without a stand the gated tests skip quietly, which is what keeps the suite usable while
/// developing. `RUSTSTREAM_REQUIRE_LIVE` turns that skip into a failure: a job that means to run
/// against a real broker sets it, so a renamed variable, a dropped `env:` block or a reordered
/// step cannot leave the whole live suite reporting success without running a single assertion.
fn test_url() -> Option<String> {
    match std::env::var("MQTT_TEST_URL") {
        Ok(url) if !url.is_empty() => Some(url),
        _ => {
            assert!(
                std::env::var_os("RUSTSTREAM_REQUIRE_LIVE").is_none(),
                "RUSTSTREAM_REQUIRE_LIVE is set, so the live conformance check must run, \
                 but MQTT_TEST_URL is missing or empty"
            );
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

/// MQTT has no batch fetch, so the batches come off the client-side buffer. The suite is what says
/// the delegation honours the size it is opened with, on the in-process transport.
#[allow(clippy::redundant_closure, clippy::redundant_closure_for_method_calls)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_in_process_broker_passes_the_batch_suite() {
    capabilities::batches(
        MqttTestBroker::new,
        |name| Name::new(name.to_owned()),
        |connected| connected.publisher(),
    )
    .await;
}

/// The same suite where the batches are filled by a real broker's deliveries rather than an
/// in-process channel, which is the only place the deadline meets a network.
#[allow(clippy::redundant_closure, clippy::redundant_closure_for_method_calls)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn mqtt_broker_passes_the_batch_suite() {
    let Some(url) = test_url() else { return };
    capabilities::batches(
        || MqttBroker::new(url.clone(), format!("batches-{}", std::process::id())),
        |name| MqttTopic::new(name),
        |connected| connected.publisher(),
    )
    .await;
}
