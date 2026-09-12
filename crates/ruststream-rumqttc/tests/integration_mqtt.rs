//! End-to-end checks against a real MQTT broker, gated behind `MQTT_TEST_URL`.
//!
//! Start one with `just brokers-up` (mosquitto), then:
//! `MQTT_TEST_URL=mqtt://127.0.0.1:1883 cargo test --all-features -- --test-threads=1`.

use std::pin::pin;
use std::time::Duration;

use futures::StreamExt;
use ruststream::runtime::PublishExt;
use ruststream::{
    AckError, Broker, ConnectedBroker, HeaderMap, IncomingMessage, Outgoing, OutgoingMessage,
    PublishPolicy, Publisher, Serialized, Subscriber,
};
use ruststream_rumqttc::{
    ConnectedMqttBroker, MqttBroker, MqttPublish, MqttPublishSteps, MqttTopic, Qos,
};

const RECV_TIMEOUT: Duration = Duration::from_secs(15);

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
                "RUSTSTREAM_REQUIRE_LIVE is set, so the live integration suite must run, \
                 but MQTT_TEST_URL is missing or empty"
            );
            eprintln!("MQTT_TEST_URL is not set; skipping the live integration test");
            None
        }
    }
}

async fn connect(url: &str, id: &str) -> ConnectedMqttBroker {
    MqttBroker::new(url, format!("it-{id}-{}", std::process::id()))
        .connect()
        .await
        .expect("broker connects")
}

fn unique(name: &str) -> String {
    format!("it/{name}/{}", std::process::id())
}

/// A payload that carries its own bytes, so a publish through the builder reaches the wire with
/// no codec in the picture: what these tests read back is the packet, not an encoding. It
/// declares no name, because every topic here is unique to the run and named at the call.
#[derive(Outgoing, Serialized)]
struct State(Vec<u8>);

impl State {
    fn new(bytes: &[u8]) -> Self {
        Self(bytes.to_vec())
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn roundtrip_preserves_payload_and_headers() {
    let Some(url) = test_url() else { return };
    let connected = connect(&url, "roundtrip").await;

    let topic = unique("roundtrip");
    let mut subscriber = connected
        .subscribe_topic(MqttTopic::new(&topic))
        .await
        .expect("subscription opens");

    let mut headers = HeaderMap::new();
    headers.insert("content-type", "application/json");
    headers.insert("x-tenant", "acme");
    headers.insert("correlation-id", "corr-1");
    let publisher = connected.publisher();
    publisher
        .publish(
            OutgoingMessage::new(&topic, b"{\"id\":1}".as_slice()).with_headers(headers),
            None,
        )
        .await
        .expect("publish succeeds");

    let mut stream = pin!(subscriber.stream());
    let message = tokio::time::timeout(RECV_TIMEOUT, stream.next())
        .await
        .expect("delivery arrives")
        .expect("stream is open")
        .expect("delivery is ok");

    assert_eq!(message.payload(), b"{\"id\":1}");
    assert_eq!(
        message.headers().get_str("content-type"),
        Some("application/json")
    );
    assert_eq!(message.headers().get_str("x-tenant"), Some("acme"));
    assert_eq!(message.headers().get_str("correlation-id"), Some("corr-1"));
    message.ack().await.expect("ack succeeds");

    connected.shutdown().await.expect("shutdown succeeds");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn wildcard_filters_match_and_report_the_real_topic() {
    let Some(url) = test_url() else { return };
    let connected = connect(&url, "wildcard").await;

    let base = unique("devices");
    let mut subscriber = connected
        .subscribe_topic(MqttTopic::new(format!("{base}/+/telemetry")))
        .await
        .expect("subscription opens");

    let publisher = connected.publisher();
    let concrete = format!("{base}/dev42/telemetry");
    publisher
        .publish(OutgoingMessage::new(&concrete, b"21.5".as_slice()), None)
        .await
        .expect("publish succeeds");

    let mut stream = pin!(subscriber.stream());
    let message = tokio::time::timeout(RECV_TIMEOUT, stream.next())
        .await
        .expect("delivery arrives")
        .expect("stream is open")
        .expect("delivery is ok");
    assert_eq!(message.topic(), concrete);
    message.ack().await.expect("ack succeeds");

    connected.shutdown().await.expect("shutdown succeeds");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn shared_subscriptions_split_the_stream() {
    let Some(url) = test_url() else { return };
    let connected = connect(&url, "shared").await;

    let topic = unique("jobs");
    let mut first = connected
        .subscribe_topic(MqttTopic::new(&topic).shared("workers"))
        .await
        .expect("first consumer subscribes");
    let mut second = connected
        .subscribe_topic(MqttTopic::new(&topic).shared("workers"))
        .await
        .expect("second consumer subscribes");

    let publisher = connected.publisher();
    for i in 0..4u8 {
        publisher
            .publish(OutgoingMessage::new(&topic, [i].as_slice()), None)
            .await
            .expect("publish succeeds");
    }

    // Between them the two consumers must see all four, each at most... the broker balances,
    // so just count across both.
    let mut seen = 0;
    let mut s1 = pin!(first.stream());
    let mut s2 = pin!(second.stream());
    while seen < 4 {
        let message = tokio::time::timeout(RECV_TIMEOUT, async {
            tokio::select! {
                m = s1.next() => m,
                m = s2.next() => m,
            }
        })
        .await
        .expect("delivery arrives")
        .expect("streams are open")
        .expect("delivery is ok");
        message.ack().await.expect("ack succeeds");
        seen += 1;
    }

    connected.shutdown().await.expect("shutdown succeeds");
}

/// The retain step is only a field of a value until a packet carries it, and only the broker can
/// say it did: a retained message is one a subscriber that was not there yet still receives.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_retain_step_reaches_the_broker() {
    let Some(url) = test_url() else { return };
    let connected = connect(&url, "retain").await;

    let topic = unique("state");
    // The publisher's own policy does not retain: the step on this one packet is what makes the
    // broker keep it for a subscriber that is not there yet.
    let publisher = connected.publisher();
    publisher
        .message(&State::new(b"online"))
        .to(&topic)
        .retain(true)
        .publish()
        .await
        .expect("publish succeeds");

    let mut subscriber = connected
        .subscribe_topic(MqttTopic::new(&topic))
        .await
        .expect("subscription opens");
    let mut stream = pin!(subscriber.stream());
    let message = tokio::time::timeout(RECV_TIMEOUT, stream.next())
        .await
        .expect("the retained message arrives on subscribe")
        .expect("stream is open")
        .expect("delivery is ok");
    assert_eq!(message.payload(), b"online");
    message.ack().await.expect("ack succeeds");

    // An empty retained payload clears the broker's stored message for the topic.
    publisher
        .message(&State::new(b""))
        .to(&topic)
        .retain(true)
        .publish()
        .await
        .expect("the retained message is cleared");

    connected.shutdown().await.expect("shutdown succeeds");
}

/// A publish that takes no step is the mirror: the policy's own retain flag is what reaches the
/// packet, so a subscriber arriving afterwards finds nothing kept for it.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_publish_with_no_step_retains_nothing() {
    let Some(url) = test_url() else { return };
    let connected = connect(&url, "no-retain").await;

    let topic = unique("plain");
    connected
        .publisher()
        .message(&State::new(b"online"))
        .to(&topic)
        .publish()
        .await
        .expect("publish succeeds");

    let mut subscriber = connected
        .subscribe_topic(MqttTopic::new(&topic))
        .await
        .expect("subscription opens");

    // A publish that did happen afterwards proves the subscription is live, so what arrives
    // first tells retained from not retained rather than slow from silent.
    connected
        .publisher()
        .message(&State::new(b"live"))
        .to(&topic)
        .publish()
        .await
        .expect("publish succeeds");

    let mut stream = pin!(subscriber.stream());
    let message = tokio::time::timeout(RECV_TIMEOUT, stream.next())
        .await
        .expect("delivery arrives")
        .expect("stream is open")
        .expect("delivery is ok");
    assert_eq!(
        message.payload(),
        b"live",
        "nothing was retained, so the first delivery is the one published after the subscribe"
    );
    message.ack().await.expect("ack succeeds");

    connected.shutdown().await.expect("shutdown succeeds");
}

/// The quality of service a step names is the one the packet is delivered under, which the
/// settlement is the proof of: a `QoS` 0 delivery could not be acknowledged at all.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_quality_of_service_step_reaches_the_broker() {
    let Some(url) = test_url() else { return };
    let connected = connect(&url, "qos-step").await;

    let topic = unique("exactly");
    let mut subscriber = connected
        .subscribe_topic(MqttTopic::new(&topic).qos(Qos::ExactlyOnce))
        .await
        .expect("subscription opens");

    // This publisher's policy publishes at QoS 0, where nothing settles; the step raises this one
    // packet to the QoS 2 handshake.
    let publisher = MqttPublish::default()
        .qos(Qos::AtMostOnce)
        .pair(&connected)
        .await
        .expect("the policy pairs with the connected broker");
    publisher
        .message(&State::new(b"once"))
        .to(&topic)
        .qos(Qos::ExactlyOnce)
        .publish()
        .await
        .expect("publish succeeds");

    let mut stream = pin!(subscriber.stream());
    let message = tokio::time::timeout(RECV_TIMEOUT, stream.next())
        .await
        .expect("delivery arrives")
        .expect("stream is open")
        .expect("delivery is ok");
    assert_eq!(message.payload(), b"once");
    message
        .ack()
        .await
        .expect("the delivery carries an acknowledgement, so the step outranked the policy");

    connected.shutdown().await.expect("shutdown succeeds");
}

/// A persistent session outlives its connection: the broker keeps the subscription and queues
/// matching messages while the subscriber is away, and the client that returns under the same id
/// receives what it missed. This is what `clean_start(false)` plus `session_expiry` buy, and it is
/// also what makes `nack(requeue = true)` report `Unsupported` rather than losing a message.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_persistent_session_replays_what_arrived_while_the_subscriber_was_away() {
    let Some(url) = test_url() else { return };

    let client_id = format!("it-session-{}", std::process::id());
    let topic = unique("session");

    let first = MqttBroker::new(url.clone(), client_id.clone())
        .clean_start(false)
        .session_expiry(Duration::from_secs(300))
        .connect()
        .await
        .expect("the first connection is accepted");

    // Only a QoS 1 subscription asks the broker to hold anything: QoS 0 has nothing to queue.
    let away = first
        .subscribe_topic(MqttTopic::new(&topic).qos(Qos::AtLeastOnce))
        .await
        .expect("subscription opens");

    // Dropping a subscriber unsubscribes its filter, which would take out of the session the
    // very subscription under test, so this one is held to the end of the test and never
    // unsubscribes over a live connection.
    first.shutdown().await.expect("shutdown succeeds");

    // A different client publishes while nobody is connected under the session's id.
    let sender = connect(&url, "session-sender").await;
    sender
        .publisher()
        .publish(OutgoingMessage::new(&topic, b"missed".as_slice()), None)
        .await
        .expect("publish succeeds");
    sender.shutdown().await.expect("shutdown succeeds");

    let resumed = MqttBroker::new(url.clone(), client_id)
        .clean_start(false)
        .session_expiry(Duration::from_secs(300))
        .connect()
        .await
        .expect("the session resumes");
    let mut subscriber = resumed
        .subscribe_topic(MqttTopic::new(&topic).qos(Qos::AtLeastOnce))
        .await
        .expect("subscription reopens");

    let mut stream = pin!(subscriber.stream());
    let message = tokio::time::timeout(RECV_TIMEOUT, stream.next())
        .await
        .expect("the queued delivery arrives on resume")
        .expect("stream is open")
        .expect("delivery is ok");
    assert_eq!(message.payload(), b"missed");
    message.ack().await.expect("ack succeeds");

    resumed.shutdown().await.expect("shutdown succeeds");
    drop(away);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn qos0_reports_ack_unsupported() {
    let Some(url) = test_url() else { return };
    let connected = connect(&url, "qos0").await;

    let topic = unique("fire");
    let mut subscriber = connected
        .subscribe_topic(MqttTopic::new(&topic).qos(Qos::AtMostOnce))
        .await
        .expect("subscription opens");

    let publisher = connected.publisher();
    publisher
        .publish(OutgoingMessage::new(&topic, b"fire".as_slice()), None)
        .await
        .expect("publish succeeds");

    let mut stream = pin!(subscriber.stream());
    let message = tokio::time::timeout(RECV_TIMEOUT, stream.next())
        .await
        .expect("delivery arrives")
        .expect("stream is open")
        .expect("delivery is ok");
    assert!(matches!(message.ack().await, Err(AckError::Unsupported)));

    connected.shutdown().await.expect("shutdown succeeds");
}

/// The two negative answers, against a server: asking for redelivery is refused because the
/// protocol cannot express it, and declining redelivery acknowledges, dropping being the only
/// terminal outcome MQTT offers. This is the behaviour a handler's `retry()` and `drop()` settle
/// through, and the stand-in twin of it is in `stand_in_mqtt.rs`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn nack_reports_unsupported_and_dropping_acknowledges() {
    let Some(url) = test_url() else { return };
    let connected = connect(&url, "nack").await;

    let topic = unique("nack");
    let mut subscriber = connected
        .subscribe_topic(MqttTopic::new(&topic).qos(Qos::AtLeastOnce))
        .await
        .expect("subscription opens");

    let publisher = connected.publisher();
    for payload in [b"requeue".as_slice(), b"drop".as_slice()] {
        publisher
            .publish(OutgoingMessage::new(&topic, payload), None)
            .await
            .expect("publish succeeds");
    }

    let mut stream = pin!(subscriber.stream());
    let first = tokio::time::timeout(RECV_TIMEOUT, stream.next())
        .await
        .expect("delivery arrives")
        .expect("stream is open")
        .expect("delivery is ok");
    assert!(matches!(first.nack(true).await, Err(AckError::Unsupported)));

    let second = tokio::time::timeout(RECV_TIMEOUT, stream.next())
        .await
        .expect("delivery arrives")
        .expect("stream is open")
        .expect("delivery is ok");
    second
        .nack(false)
        .await
        .expect("declining redelivery acknowledges");

    connected.shutdown().await.expect("shutdown succeeds");
}
