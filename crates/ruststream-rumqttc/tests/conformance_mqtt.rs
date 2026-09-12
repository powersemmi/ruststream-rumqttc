//! Conformance: every suite this broker's capabilities justify, run twice where it can be - once
//! against the in-process transport and once against a real server (gated behind `MQTT_TEST_URL`).
//!
//! Running them in process is what says the stand-in obeys the framework's own definition of a
//! broker rather than a convenient subset of it; running them live is what says the stand-in is
//! not lying. The routing suite is in-process only by construction: it drives
//! [`TestableBroker`](ruststream::testing::TestableBroker), which no server implements.
//!
//! Start one with `just brokers-up` (mosquitto), then:
//! `MQTT_TEST_URL=mqtt://127.0.0.1:1883 cargo test --all-features`.

#![cfg(feature = "testing")]

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

/// The ladder the framework defines, walked on the transport a service's tests actually run on:
/// synchronous construction, `connect`, a subscription opened through the crate's own descriptor,
/// a publish it receives and settles, `shutdown` - and then the assertion that gives the suite its
/// teeth here, that a publisher handed out before the shutdown reports the closed connection
/// instead of quietly accepting messages nobody will ever read.
#[allow(clippy::redundant_closure, clippy::redundant_closure_for_method_calls)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_in_process_broker_passes_lifecycle() {
    harness::lifecycle(
        MqttTestBroker::new,
        |name| MqttTopic::new(name),
        |connected| connected.publisher(),
    )
    .await;
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
/// the delegation honours the size it is opened with, on the in-process transport - through the
/// crate's own descriptor, the same one the live leg below opens with.
#[allow(clippy::redundant_closure, clippy::redundant_closure_for_method_calls)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_in_process_broker_passes_the_batch_suite() {
    capabilities::batches(
        MqttTestBroker::new,
        |name| MqttTopic::new(name),
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
