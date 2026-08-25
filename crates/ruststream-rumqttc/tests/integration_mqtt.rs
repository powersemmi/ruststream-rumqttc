//! End-to-end checks against a real MQTT broker, gated behind `MQTT_TEST_URL`.
//!
//! Start one with `just brokers-up` (mosquitto), then:
//! `MQTT_TEST_URL=mqtt://127.0.0.1:1883 cargo test --all-features -- --test-threads=1`.

use std::pin::pin;
use std::time::Duration;

use futures::StreamExt;
use ruststream::{
    AckError, Broker, ConnectedBroker, Headers, IncomingMessage, OutgoingMessage, Publisher,
    Subscriber,
};
use ruststream_rumqttc::{ConnectedMqttBroker, MqttBroker, MqttPublishOptions, MqttTopic, Qos};

const RECV_TIMEOUT: Duration = Duration::from_secs(15);

fn test_url() -> Option<String> {
    match std::env::var("MQTT_TEST_URL") {
        Ok(url) if !url.is_empty() => Some(url),
        _ => {
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

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn roundtrip_preserves_payload_and_headers() {
    let Some(url) = test_url() else { return };
    let connected = connect(&url, "roundtrip").await;

    let topic = unique("roundtrip");
    let mut subscriber = connected
        .subscribe_topic(MqttTopic::new(&topic))
        .await
        .expect("subscription opens");

    let mut headers = Headers::new();
    headers.insert("content-type", "application/json");
    headers.insert("x-tenant", "acme");
    headers.insert("correlation-id", "corr-1");
    let publisher = connected.publisher();
    publisher
        .publish(OutgoingMessage::new(&topic, b"{\"id\":1}".as_slice()).with_headers(headers))
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
        .publish(OutgoingMessage::new(&concrete, b"21.5".as_slice()))
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
            .publish(OutgoingMessage::new(&topic, [i].as_slice()))
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

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_per_publish_retain_override_reaches_a_later_subscriber() {
    let Some(url) = test_url() else { return };
    let connected = connect(&url, "retain").await;

    let topic = unique("state");
    // The publisher's own policy does not retain: the flag on this one packet is what makes the
    // broker keep it for a subscriber that is not there yet.
    let publisher = connected.publisher();
    publisher
        .with_retain(true)
        .publish(OutgoingMessage::new(&topic, b"online".as_slice()))
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
        .with_retain(true)
        .publish(OutgoingMessage::new(&topic, b"".as_slice()))
        .await
        .expect("the retained message is cleared");

    connected.shutdown().await.expect("shutdown succeeds");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_per_publish_qos_override_settles_through_the_protocol() {
    let Some(url) = test_url() else { return };
    let connected = connect(&url, "qos-override").await;

    let topic = unique("exactly");
    let mut subscriber = connected
        .subscribe_topic(MqttTopic::new(&topic).qos(Qos::ExactlyOnce))
        .await
        .expect("subscription opens");

    // The publisher's policy is QoS 1; the override raises this packet to the QoS 2 handshake.
    connected
        .publisher()
        .with_qos(Qos::ExactlyOnce)
        .publish(OutgoingMessage::new(&topic, b"once".as_slice()))
        .await
        .expect("publish succeeds");

    let mut stream = pin!(subscriber.stream());
    let message = tokio::time::timeout(RECV_TIMEOUT, stream.next())
        .await
        .expect("delivery arrives")
        .expect("stream is open")
        .expect("delivery is ok");
    assert_eq!(message.payload(), b"once");
    message.ack().await.expect("ack succeeds");

    connected.shutdown().await.expect("shutdown succeeds");
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
        .publish(OutgoingMessage::new(&topic, b"fire".as_slice()))
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
