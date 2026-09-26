//! Several subscriptions on one connection whose filters meet, against a real MQTT broker, gated
//! behind `MQTT_TEST_URL`.
//!
//! The subject is the connection's demultiplexing of what the server sends, so it runs where the
//! server decides how many packets a publish becomes: `just test-brokers` (mosquitto).

use std::pin::pin;
use std::time::Duration;

use futures::StreamExt;
use ruststream::{
    Broker, ConnectedBroker, IncomingMessage, OutgoingMessage, Publisher, Subscriber,
};
use ruststream_rumqttc::{
    ConnectedMqttBroker, MqttBroker, MqttFilter, MqttPublishOptions, MqttSubscriber, MqttTopic,
};

const RECV_TIMEOUT: Duration = Duration::from_secs(15);
/// How long a read waits to be sure no further copy follows the last delivery it expected.
const QUIET: Duration = Duration::from_millis(300);

/// The live broker URL, or `None` when there is no stand; `RUSTSTREAM_REQUIRE_LIVE` turns the skip
/// into a failure, so a job that means to run live cannot pass without running.
fn test_url() -> Option<String> {
    match std::env::var("MQTT_TEST_URL") {
        Ok(url) if !url.is_empty() => Some(url),
        _ => {
            assert!(
                std::env::var_os("RUSTSTREAM_REQUIRE_LIVE").is_none(),
                "RUSTSTREAM_REQUIRE_LIVE is set, so the live overlap suite must run, \
                 but MQTT_TEST_URL is missing or empty"
            );
            eprintln!("MQTT_TEST_URL is not set; skipping the live overlap test");
            None
        }
    }
}

async fn connect(url: &str, id: &str) -> ConnectedMqttBroker {
    MqttBroker::new(url, format!("overlap-{id}-{}", std::process::id()))
        .connect()
        .await
        .expect("broker connects")
}

fn unique(name: &str) -> String {
    format!("overlap/{name}/{}", std::process::id())
}

async fn publish(connected: &ConnectedMqttBroker, topic: &str, payload: &[u8]) {
    connected
        .publisher()
        .publish(OutgoingMessage::new(topic, payload), None)
        .await
        .expect("publish succeeds");
}

async fn publish_retained(connected: &ConnectedMqttBroker, topic: &str, payload: &[u8]) {
    connected
        .publisher()
        .publish(
            OutgoingMessage::new(topic, payload),
            Some(&MqttPublishOptions::default().retain(true)),
        )
        .await
        .expect("retained publish succeeds");
}

/// Reads `subscriber` up to and including the delivery whose payload is `last`, acknowledging each
/// one, and answers the payloads in arrival order with whether each acknowledgement was accepted.
///
/// The server sends one connection's packets in order, so every copy of what was published before
/// `last` has arrived by the time `last` does; a second copy of `last` itself would come after it,
/// so the read then checks that nothing more arrives.
async fn read_through(subscriber: &mut MqttSubscriber, last: &str) -> Vec<(String, bool)> {
    let mut stream = pin!(subscriber.stream());
    let mut seen = Vec::new();
    loop {
        let message = tokio::time::timeout(RECV_TIMEOUT, stream.next())
            .await
            .unwrap_or_else(|_| panic!("delivery arrives; so far {seen:?}"))
            .expect("stream is open")
            .expect("delivery is ok");
        let payload = String::from_utf8_lossy(message.payload()).into_owned();
        let acknowledged = message.ack().await.is_ok();
        let done = payload == last;
        seen.push((payload, acknowledged));
        if done {
            let trailing = tokio::time::timeout(QUIET, stream.next()).await;
            assert!(
                trailing.is_err(),
                "a delivery arrived after {last:?}: {seen:?} then more"
            );
            return seen;
        }
    }
}

/// One publish that two overlapping filters match reaches each of their subscriptions once, and
/// each delivery settles its own copy on the wire.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn overlapping_filters_deliver_each_publish_once_per_subscription() {
    let Some(url) = test_url() else { return };
    let connected = connect(&url, "filters").await;

    let base = unique("filters");
    let mut single = connected
        .subscribe_filter(MqttFilter::new(format!("{base}/+")))
        .await
        .expect("the single-level filter opens");
    let mut every = connected
        .subscribe_filter(MqttFilter::new(format!("{base}/#")))
        .await
        .expect("the multi-level filter opens");

    let topic = format!("{base}/device");
    publish(&connected, &topic, b"first").await;
    publish(&connected, &topic, b"last").await;

    let expected = vec![("first".to_owned(), true), ("last".to_owned(), true)];
    assert_eq!(read_through(&mut single, "last").await, expected);
    assert_eq!(read_through(&mut every, "last").await, expected);

    connected.shutdown().await.expect("shutdown succeeds");
}

/// A share group's subscription and a plain one on the same filter are two subscriptions at the
/// server, and each receives a publish once.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_share_group_next_to_a_plain_subscription_receives_each_publish_once() {
    let Some(url) = test_url() else { return };
    let connected = connect(&url, "group").await;

    let topic = unique("group");
    let mut plain = connected
        .subscribe_topic(MqttTopic::new(&topic))
        .await
        .expect("the plain subscription opens");
    let mut grouped = connected
        .subscribe_topic(MqttTopic::new(&topic).shared("overlap"))
        .await
        .expect("the group subscription opens");

    publish(&connected, &topic, b"first").await;
    publish(&connected, &topic, b"last").await;

    let expected = vec![("first".to_owned(), true), ("last".to_owned(), true)];
    assert_eq!(read_through(&mut plain, "last").await, expected);
    assert_eq!(read_through(&mut grouped, "last").await, expected);

    connected.shutdown().await.expect("shutdown succeeds");
}

/// Dropping one of two subscriptions on one filter leaves the filter subscribed at the server for
/// the other.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn dropping_one_of_two_subscriptions_on_a_filter_keeps_the_other_receiving() {
    let Some(url) = test_url() else { return };
    let connected = connect(&url, "shared-filter").await;

    let topic = unique("shared-filter");
    let first = connected
        .subscribe_topic(MqttTopic::new(&topic))
        .await
        .expect("the first subscription opens");
    let mut survivor = connected
        .subscribe_topic(MqttTopic::new(&topic))
        .await
        .expect("the second subscription opens");
    // Whatever request the drop sends goes out ahead of the publish, on the same connection, so the
    // server has acted on it before the publish arrives.
    drop(first);

    publish(&connected, &topic, b"only").await;

    assert_eq!(
        read_through(&mut survivor, "only").await,
        vec![("only".to_owned(), true)]
    );

    connected.shutdown().await.expect("shutdown succeeds");
}

/// The last subscription on a filter to go takes the filter with it: a new subscription on an
/// overlapping filter afterwards receives a publish once, not once more for the filter that is
/// gone.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_last_subscription_on_a_filter_to_go_unsubscribes_it() {
    let Some(url) = test_url() else { return };
    let connected = connect(&url, "last-goes").await;

    let base = unique("last-goes");
    let gone = connected
        .subscribe_filter(MqttFilter::new(format!("{base}/#")))
        .await
        .expect("the filter that goes opens");
    drop(gone);
    let mut remaining = connected
        .subscribe_filter(MqttFilter::new(format!("{base}/+")))
        .await
        .expect("the remaining filter opens");

    let topic = format!("{base}/device");
    publish(&connected, &topic, b"first").await;
    publish(&connected, &topic, b"last").await;

    assert_eq!(
        read_through(&mut remaining, "last").await,
        vec![("first".to_owned(), true), ("last".to_owned(), true)]
    );

    connected.shutdown().await.expect("shutdown succeeds");
}

/// A retained message reaches each overlapping subscription once: a filter opened later does not
/// hand the ones opened before it a second copy of what they received on subscribe.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_retained_message_reaches_each_overlapping_subscription_once() {
    let Some(url) = test_url() else { return };
    let connected = connect(&url, "retained").await;

    let base = unique("retained");
    let topic = format!("{base}/device");
    publish_retained(&connected, &topic, b"kept").await;
    let mut single = connected
        .subscribe_filter(MqttFilter::new(format!("{base}/+")))
        .await
        .expect("the single-level filter opens");
    let mut every = connected
        .subscribe_filter(MqttFilter::new(format!("{base}/#")))
        .await
        .expect("the multi-level filter opens");
    publish(&connected, &topic, b"last").await;

    let expected = vec![("kept".to_owned(), true), ("last".to_owned(), true)];
    let received = (
        read_through(&mut single, "last").await,
        read_through(&mut every, "last").await,
    );
    // An empty retained payload clears the topic, so a later run starts from nothing.
    publish_retained(&connected, &topic, b"").await;
    assert_eq!(received, (expected.clone(), expected));

    connected.shutdown().await.expect("shutdown succeeds");
}
