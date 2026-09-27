//! Conformance: every suite this broker's capabilities justify, run twice over the same production
//! broker - once connected in process and once against a real server (gated behind
//! `MQTT_TEST_URL`).
//!
//! Running them in process is what says the in-process mode obeys the framework's own definition
//! of a broker rather than a convenient subset of it; running them live is what says it is not
//! lying. The suites that compare the two transports, and the one that needs several connections
//! to reach one server, run live only.
//!
//! Start one with `just brokers-up` (mosquitto), then:
//! `MQTT_TEST_URL=mqtt://127.0.0.1:1883 cargo test --all-features`.

#![cfg(feature = "testing")]

use std::time::Duration;

use ruststream::Name;
use ruststream::conformance::harness::{self, InProcessBroker};
use ruststream::conformance::in_process::{self, Refusal};
use ruststream::conformance::message_shape::{self, OptionCases};
use ruststream::conformance::{
    capabilities, helpers::unique_subject, lifecycle, retry, settlement,
};
use ruststream::testing::Backlog;
use ruststream_rumqttc::{
    ConnectedMqttBroker, MqttBroker, MqttMessage, MqttPublish, MqttPublishOptions, MqttTopic, Qos,
};

/// The production broker a service builds; the in-process passes dial nothing.
fn broker() -> MqttBroker {
    MqttBroker::new("mqtt://localhost:1883", "conformance")
}

/// The production broker, connected in process by the suites that take any broker.
fn in_process() -> InProcessBroker<MqttBroker> {
    InProcessBroker::new(broker())
}

/// A broker on the stand at `url` with a client id no other connection of the run uses, so two
/// connections a suite holds at once do not take each other's session over.
fn live(url: &str, suite: &str) -> MqttBroker {
    MqttBroker::new(url, unique_subject(suite))
}

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

/// The ladder the framework defines, walked on the in-process mode a service's tests run on:
/// synchronous construction, `connect`, a subscription opened through the crate's own descriptor,
/// publishes it receives and settles from runtimes of their own, `shutdown` - and then the
/// assertion that gives the suite its teeth here, that a publisher handed out before the shutdown
/// reports the closed connection instead of quietly accepting messages nobody will ever read.
#[allow(clippy::redundant_closure, clippy::redundant_closure_for_method_calls)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_in_process_broker_passes_lifecycle() {
    harness::lifecycle(
        in_process,
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
        move || live(&url, "lifecycle"),
        |name| MqttTopic::new(name),
        |connected| connected.publisher(),
    )
    .await;
}

/// An acknowledgement and a publish handed over right before `shutdown` reach the server. MQTT is
/// publish/subscribe, so the observer is a subscription open on another connection. Live only:
/// each broker connected in process is a server of its own, and the check needs two connections
/// to reach one.
#[allow(clippy::redundant_closure, clippy::redundant_closure_for_method_calls)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn mqtt_broker_flushes_on_shutdown() {
    let Some(url) = test_url() else { return };
    lifecycle::shutdown_flushes(
        || live(&url, "flush"),
        |name| MqttTopic::new(name),
        |connected| connected.publisher(),
        Backlog::Missed,
    )
    .await;
}

/// What a descriptor that addresses its own copies promises: a publish to the address it reports
/// reaches the subscription that reported it. `MqttTopic` reports the topic, share group and all,
/// and a bare name reports itself, so both are held to it.
#[allow(clippy::redundant_closure, clippy::redundant_closure_for_method_calls)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_in_process_broker_passes_the_redelivery_address_suite() {
    retry::redelivery_address(
        in_process,
        |name| MqttTopic::new(name),
        |connected| connected.publisher(),
    )
    .await;
    retry::redelivery_address(
        in_process,
        |name| Name::new(name.to_owned()),
        |connected| connected.publisher(),
    )
    .await;
}

/// The same promise against a server, which is where a retry copy actually travels.
#[allow(clippy::redundant_closure, clippy::redundant_closure_for_method_calls)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn mqtt_broker_passes_the_redelivery_address_suite() {
    let Some(url) = test_url() else { return };
    retry::redelivery_address(
        || live(&url, "redelivery"),
        |name| MqttTopic::new(name),
        |connected| connected.publisher(),
    )
    .await;
    retry::redelivery_address(
        || live(&url, "redelivery-name"),
        |name| Name::new(name.to_owned()),
        |connected| connected.publisher(),
    )
    .await;
}

/// What each settlement answers on the server, held to its meaning there, and the same answers
/// from the in-process transport. MQTT has no negative acknowledgement, so the requeue answers
/// `Unsupported` on both.
#[allow(clippy::redundant_closure, clippy::redundant_closure_for_method_calls)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn mqtt_broker_settles_like_the_in_process_transport() {
    let Some(url) = test_url() else { return };
    settlement::matches_in_process(
        || live(&url, "settlement"),
        |name| MqttTopic::new(name),
        |connected| connected.publisher(),
        Duration::ZERO,
    )
    .await;
}

/// A message published before a subscription opens reaches it on neither transport, as the
/// in-process mode declares.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn mqtt_broker_keeps_the_backlog_the_in_process_transport_declares() {
    let Some(url) = test_url() else { return };
    in_process::backlog_matches_server(|| live(&url, "backlog"), ConnectedMqttBroker::publisher)
        .await;
}

/// What the server refuses, the in-process transport refuses too: a publish to a wildcard, which
/// the protocol forbids in a topic name, and a topic descriptor naming one.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn mqtt_broker_refuses_what_the_in_process_transport_refuses() {
    let Some(url) = test_url() else { return };
    in_process::refuses_like_the_server(
        || live(&url, "refusals"),
        ConnectedMqttBroker::publisher,
        [
            Refusal::Publish {
                name: "conformance/+/refused".to_owned(),
            },
            Refusal::Subscription {
                source: MqttTopic::new("conformance/#"),
            },
        ],
    )
    .await;
}

/// What a publish may override of its policy, and what a delivery then shows. The policy sends
/// at `ExactlyOnce`, which the client would not pick by itself, and the subscription takes every
/// level, so the delivery's quality of service is the one the publish carried. MQTT honours every
/// level, so nothing is refused.
fn option_cases() -> OptionCases<MqttPublishOptions, Qos> {
    OptionCases::new(Qos::ExactlyOnce)
        .overrides(
            MqttPublishOptions::default().qos(Qos::AtMostOnce),
            Qos::AtMostOnce,
        )
        .overrides(
            MqttPublishOptions::default().qos(Qos::AtLeastOnce),
            Qos::AtLeastOnce,
        )
        // A retain flag left as the policy has it keeps the policy's quality of service.
        .overrides(
            MqttPublishOptions::default().retain(false),
            Qos::ExactlyOnce,
        )
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_in_process_broker_resolves_publish_options() {
    message_shape::publish_options(
        in_process,
        &unique_subject("conformance.options"),
        |name| MqttTopic::new(name).qos(Qos::ExactlyOnce),
        MqttPublish::default().qos(Qos::ExactlyOnce),
        option_cases(),
        MqttMessage::qos,
    )
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn mqtt_broker_resolves_publish_options() {
    let Some(url) = test_url() else { return };
    message_shape::publish_options(
        || live(&url, "options"),
        &unique_subject("conformance.options"),
        |name| MqttTopic::new(name).qos(Qos::ExactlyOnce),
        MqttPublish::default().qos(Qos::ExactlyOnce),
        option_cases(),
        MqttMessage::qos,
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
        in_process,
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
        || live(&url, "batches"),
        |name| MqttTopic::new(name),
        |connected| connected.publisher(),
    )
    .await;
}
