//! The framework's retry fallback on MQTT, each test body run twice: in process under `just test`,
//! and against the stand under `just test-brokers` (gated behind `MQTT_TEST_URL`).
//!
//! MQTT defers nothing and counts nothing, so every one of these runs on the runtime's own
//! fallback: the copy is an ordinary publish and the attempt number is an ordinary header. Only
//! the start call differs between the two modes; the app is the one a service builds, on
//! `MqttBroker`.

#![cfg(feature = "testing")]

use std::time::Duration;

// The derive and the value a publish transform reads share the name in different namespaces: the
// prelude carries the macro `ruststream::Outgoing`, and this is the type `runtime::Outgoing`.
use ruststream::runtime::{Outgoing, PublishContext, RETRY_COUNT_HEADER};
use ruststream::testing::TestApp;
use ruststream_rumqttc::prelude::*;
use serde::{Deserialize, Serialize};

/// The address the in-process mode is built with; it dials nothing.
const IN_PROCESS: &str = "mqtt://localhost:1883";

/// Long enough to be a delay the runtime actually waits out, short enough to keep the live run
/// quick. In process the clock is paused, so nothing waits for it there.
const RETRY_DELAY: Duration = Duration::from_millis(300);

/// The live broker URL, or `None` when there is no stand to run against.
///
/// Without a stand the live legs skip quietly, which is what keeps the suite usable while
/// developing. `RUSTSTREAM_REQUIRE_LIVE` turns that skip into a failure: a job that means to run
/// against a real broker sets it, so a renamed variable cannot leave the live legs reporting
/// success without running a single assertion.
fn live_url() -> Option<String> {
    match std::env::var("MQTT_TEST_URL") {
        Ok(url) if !url.is_empty() => Some(url),
        _ => {
            assert!(
                std::env::var_os("RUSTSTREAM_REQUIRE_LIVE").is_none(),
                "RUSTSTREAM_REQUIRE_LIVE is set, so the live legs must run, \
                 but MQTT_TEST_URL is missing or empty"
            );
            eprintln!("MQTT_TEST_URL is not set; skipping the live leg");
            None
        }
    }
}

/// The broker a service builds its app on, with a client id no other run shares.
fn broker(url: &str, service: &str) -> MqttBroker {
    MqttBroker::new(url, format!("{service}-{}", std::process::id()))
}

// --- A deferred retry comes back carrying its count. ---

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
#[outgoing(name = "both/retry/deferred")]
struct Reading {
    device: String,
}

/// Defers its first delivery and settles the copy, so one subscription runs both legs of the
/// fallback.
#[subscriber(MqttTopic::new("both/retry/deferred").qos(Qos::AtLeastOnce))]
async fn defer_once(reading: &Reading, ctx: &mut Context) -> HandlerOutcome {
    let _ = &reading.device;
    if ctx.headers().get_str(RETRY_COUNT_HEADER).is_none() {
        return HandlerOutcome::retry_after(RETRY_DELAY);
    }
    HandlerOutcome::ack()
}

fn deferring_app(url: &str) -> RustStream {
    RustStream::new(AppInfo::new("both-retry", "0.1.0")).with_broker(
        broker(url, "both-retry"),
        |b| {
            b.include(defer_once);
        },
    )
}

/// `retry_after` on MQTT is a publish: the runtime acknowledges the original, waits, and sends a
/// copy to the topic the subscription reported, carrying the attempt number as a header.
async fn a_deferred_retry_returns_carrying_its_count(tb: TestApp<()>) {
    let reading = Reading {
        device: "dev42".to_owned(),
    };
    tb.broker::<MqttBroker>()
        .message(&reading)
        .publish()
        .await
        .expect("the reading reaches the service");
    tb.broker::<MqttBroker>()
        .subscriber("both/retry/deferred")
        .assert_called_once()
        .settled(HandlerOutcome::retry_after(RETRY_DELAY));

    tb.advance(RETRY_DELAY)
        .await
        .expect("the copy comes back and is handled");

    tb.broker::<MqttBroker>()
        .subscriber("both/retry/deferred")
        .assert_called(2)
        .with(&reading)
        .settled(HandlerOutcome::ack());
    tb.broker::<MqttBroker>()
        .published::<Reading>("both/retry/deferred")
        .assert_called(2)
        .with_header(RETRY_COUNT_HEADER, "1");

    tb.shutdown().await.expect("the service stops");
}

#[tokio::test(start_paused = true)]
async fn a_deferred_retry_returns_carrying_its_count_in_process() {
    let tb = TestApp::start(deferring_app(IN_PROCESS))
        .await
        .expect("the harness starts");
    a_deferred_retry_returns_carrying_its_count(tb).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_deferred_retry_returns_carrying_its_count_live() {
    let Some(url) = live_url() else { return };
    let tb = TestApp::start_live(deferring_app(&url))
        .await
        .expect("the harness starts against the stand");
    a_deferred_retry_returns_carrying_its_count(tb).await;
}

// --- A spent cap sends the delivery to the dead-letter topic. ---

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
#[outgoing(name = "both/retry/capped")]
struct Command {
    id: u64,
}

/// Never ready, which is what a cap is for.
#[subscriber(MqttTopic::new("both/retry/capped").qos(Qos::AtLeastOnce))]
async fn never_settles(command: &Command) -> HandlerOutcome {
    let _ = command.id;
    HandlerOutcome::retry_after(RETRY_DELAY)
}

/// The end of the run: the delivery that spent its attempts lands here, on a topic no other
/// subscription of this service reads.
#[subscriber(MqttTopic::new("both/retry/dead").qos(Qos::AtLeastOnce))]
async fn collect_spent(command: &Command) -> HandlerOutcome {
    let _ = command.id;
    HandlerOutcome::ack()
}

fn capped_app(url: &str) -> RustStream {
    RustStream::new(AppInfo::new("both-capped", "0.1.0")).with_broker(
        broker(url, "both-capped"),
        |b| {
            b.include(never_settles)
                .max_attempts(nonzero!(2u32))
                .dead_letter("both/retry/dead");
            b.include(collect_spent);
        },
    )
}

/// MQTT counts no redeliveries, so the cap is counted on the header the copies carry and the
/// dead-letter topic is an ordinary publish, which the service's own subscription reads back.
async fn a_spent_cap_sends_the_delivery_to_the_dead_letter_topic(tb: TestApp<()>) {
    tb.broker::<MqttBroker>()
        .message(&Command { id: 11 })
        .publish()
        .await
        .expect("the command reaches the service");
    tb.advance(RETRY_DELAY)
        .await
        .expect("the copy comes back and is handled");
    tb.advance(RETRY_DELAY)
        .await
        .expect("the spent delivery leaves for the dead-letter topic");

    tb.broker::<MqttBroker>()
        .subscriber("both/retry/capped")
        .assert_called(2);
    tb.broker::<MqttBroker>()
        .published::<Command>("both/retry/dead")
        .assert_called_once()
        .with(&Command { id: 11 });
    tb.broker::<MqttBroker>()
        .subscriber("both/retry/dead")
        .assert_called_once()
        .with(&Command { id: 11 })
        .settled(HandlerOutcome::ack());

    tb.shutdown().await.expect("the service stops");
}

#[tokio::test(start_paused = true)]
async fn a_spent_cap_sends_the_delivery_to_the_dead_letter_topic_in_process() {
    let tb = TestApp::start(capped_app(IN_PROCESS))
        .await
        .expect("the harness starts");
    a_spent_cap_sends_the_delivery_to_the_dead_letter_topic(tb).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_spent_cap_sends_the_delivery_to_the_dead_letter_topic_live() {
    let Some(url) = live_url() else { return };
    let tb = TestApp::start_live(capped_app(&url))
        .await
        .expect("the harness starts against the stand");
    a_spent_cap_sends_the_delivery_to_the_dead_letter_topic(tb).await;
}

// --- A copy returns to the topic its delivery arrived on. ---

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
#[outgoing(name = "both/fleet/42/deferred")]
struct FleetReading {
    device: String,
}

/// Sends every copy back to the topic its own delivery arrived on, which is the one topic of the
/// filter's many that this message belongs to.
struct ToDeliveryTopic;

impl<Options> PublishTransform<ForReply<MqttContext>, Options> for ToDeliveryTopic {
    type Destination = Names;

    fn apply(
        &self,
        out: &mut Outgoing<'_>,
        _options: &mut Option<Options>,
        cx: &PublishContext<'_, MqttContext>,
    ) {
        out.set_name(cx.context(DeliveryTopic).to_owned());
    }
}

/// Defers the first delivery and settles the copy, reading the topic it arrived on so the
/// registration's context type is the crate's.
#[subscriber(MqttFilter::new("both/fleet/+/deferred").qos(Qos::AtLeastOnce))]
async fn reconcile_per_device(
    reading: &FleetReading,
    ctx: &mut Context<'_, MqttContext>,
) -> HandlerOutcome {
    let _ = &reading.device;
    if ctx.headers().get_str(RETRY_COUNT_HEADER).is_none() {
        return HandlerOutcome::retry_after(RETRY_DELAY);
    }
    HandlerOutcome::ack()
}

fn fleet_app(url: &str) -> RustStream {
    RustStream::new(AppInfo::new("both-fleet", "0.1.0")).with_broker(
        broker(url, "both-fleet"),
        |b| {
            b.include(reconcile_per_device)
                .out_retry(Publish::default())
                .transform(ToDeliveryTopic);
        },
    )
}

/// A filter subscription reads many topics and is a name nobody can publish to, so a deferred
/// copy is addressed per delivery. The transform reads the delivery's own topic out of the
/// broker's context, and the copy comes back on the device it came from, through the filter.
async fn a_copy_returns_to_the_topic_its_delivery_arrived_on(tb: TestApp<()>) {
    tb.broker::<MqttBroker>()
        .message(&FleetReading {
            device: "42".to_owned(),
        })
        .publish()
        .await
        .expect("the reading reaches the service");
    tb.advance(RETRY_DELAY)
        .await
        .expect("the copy comes back and is handled");

    tb.broker::<MqttBroker>()
        .published::<FleetReading>("both/fleet/42/deferred")
        .assert_called(2)
        .with_header(RETRY_COUNT_HEADER, "1");
    tb.broker::<MqttBroker>()
        .published::<FleetReading>("both/fleet/+/deferred")
        .assert_not_called();
    tb.broker::<MqttBroker>()
        .subscriber("both/fleet/+/deferred")
        .assert_called(2)
        .settled(HandlerOutcome::ack());

    tb.shutdown().await.expect("the service stops");
}

#[tokio::test(start_paused = true)]
async fn a_copy_returns_to_the_topic_its_delivery_arrived_on_in_process() {
    let tb = TestApp::start(fleet_app(IN_PROCESS))
        .await
        .expect("the harness starts");
    a_copy_returns_to_the_topic_its_delivery_arrived_on(tb).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_copy_returns_to_the_topic_its_delivery_arrived_on_live() {
    let Some(url) = live_url() else { return };
    let tb = TestApp::start_live(fleet_app(&url))
        .await
        .expect("the harness starts against the stand");
    a_copy_returns_to_the_topic_its_delivery_arrived_on(tb).await;
}
