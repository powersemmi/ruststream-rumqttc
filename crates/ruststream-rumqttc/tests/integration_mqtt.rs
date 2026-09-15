//! End-to-end checks against a real MQTT broker, gated behind `MQTT_TEST_URL`.
//!
//! Start one with `just brokers-up` (mosquitto), then:
//! `MQTT_TEST_URL=mqtt://127.0.0.1:1883 cargo test --all-features -- --test-threads=1`.

use std::path::{Path, PathBuf};
use std::pin::pin;
use std::time::Duration;

use futures::StreamExt;
use rumqttc::v5::mqttbytes::QoS;
use rumqttc::v5::mqttbytes::v5::{Packet, Publish};
use rumqttc::v5::{AsyncClient, Event, EventLoop, MqttOptions};
use ruststream::runtime::PublishExt;
use ruststream::{
    AckError, Broker, ConnectedBroker, HeaderMap, IncomingMessage, Outgoing, OutgoingMessage,
    PublishPolicy, Publisher, Serialized, ServerSpec, Subscriber,
};
use ruststream_rumqttc::{
    ConnectedMqttBroker, MqttBroker, MqttError, MqttFilter, MqttPublish, MqttPublishSteps,
    MqttTopic, Qos,
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
        .subscribe_filter(MqttFilter::new(format!("{base}/+/telemetry")))
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

/// Competing consumers, each on its own connection, which is where the broker does the splitting
/// rather than this client: a group member takes a message and the others do not see it. The
/// count is exact on purpose - a broker that fanned the group out would deliver eight.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_shared_subscription_hands_each_message_to_one_member() {
    let Some(url) = test_url() else { return };
    let topic = unique("jobs");

    let left = connect(&url, "shared-left").await;
    let right = connect(&url, "shared-right").await;
    let mut first = left
        .subscribe_topic(MqttTopic::new(&topic).shared("workers"))
        .await
        .expect("the first member subscribes");
    let mut second = right
        .subscribe_topic(MqttTopic::new(&topic).shared("workers"))
        .await
        .expect("the second member subscribes");

    let sender = connect(&url, "shared-sender").await;
    let publisher = sender.publisher();
    for i in 0..4u8 {
        publisher
            .publish(OutgoingMessage::new(&topic, [i].as_slice()), None)
            .await
            .expect("publish succeeds");
    }

    let mut left_seen = Vec::new();
    let mut right_seen = Vec::new();
    let mut s1 = pin!(first.stream());
    let mut s2 = pin!(second.stream());
    while left_seen.len() + right_seen.len() < 4 {
        tokio::time::timeout(RECV_TIMEOUT, async {
            tokio::select! {
                Some(message) = s1.next() => left_seen.push(message.expect("delivery is ok")),
                Some(message) = s2.next() => right_seen.push(message.expect("delivery is ok")),
            }
        })
        .await
        .expect("delivery arrives");
    }
    assert!(
        !left_seen.is_empty() && !right_seen.is_empty(),
        "the broker distributes across the group rather than pinning it to one member"
    );

    let mut taken: Vec<u8> = left_seen
        .iter()
        .chain(right_seen.iter())
        .map(|message| message.payload()[0])
        .collect();
    taken.sort_unstable();
    assert_eq!(
        taken,
        vec![0, 1, 2, 3],
        "each message went to exactly one member of the group"
    );
    for message in left_seen.into_iter().chain(right_seen) {
        message.ack().await.expect("ack succeeds");
    }
    assert!(
        tokio::time::timeout(QUIET, async {
            tokio::select! {
                message = s1.next() => message,
                message = s2.next() => message,
            }
        })
        .await
        .is_err(),
        "the group is done: nothing was delivered twice"
    );

    left.shutdown().await.expect("shutdown succeeds");
    right.shutdown().await.expect("shutdown succeeds");
    sender.shutdown().await.expect("shutdown succeeds");
}

/// The contrast that gives the share group its meaning: the same two consumers without one are
/// two independent subscriptions, and each receives every message.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn subscriptions_outside_a_group_each_receive_every_message() {
    let Some(url) = test_url() else { return };
    let topic = unique("fanout");

    let left = connect(&url, "fanout-left").await;
    let right = connect(&url, "fanout-right").await;
    let mut first = left
        .subscribe_topic(MqttTopic::new(&topic))
        .await
        .expect("the first consumer subscribes");
    let mut second = right
        .subscribe_topic(MqttTopic::new(&topic))
        .await
        .expect("the second consumer subscribes");

    let sender = connect(&url, "fanout-sender").await;
    let publisher = sender.publisher();
    for i in 0..2u8 {
        publisher
            .publish(OutgoingMessage::new(&topic, [i].as_slice()), None)
            .await
            .expect("publish succeeds");
    }

    for subscriber in [&mut first, &mut second] {
        let mut stream = pin!(subscriber.stream());
        let mut seen = Vec::new();
        while seen.len() < 2 {
            let message = tokio::time::timeout(RECV_TIMEOUT, stream.next())
                .await
                .expect("delivery arrives")
                .expect("stream is open")
                .expect("delivery is ok");
            seen.push(message.payload()[0]);
            message.ack().await.expect("ack succeeds");
        }
        assert_eq!(
            seen,
            vec![0, 1],
            "a subscription of its own reads all of them"
        );
    }

    left.shutdown().await.expect("shutdown succeeds");
    right.shutdown().await.expect("shutdown succeeds");
    sender.shutdown().await.expect("shutdown succeeds");
}

/// A retained message is the broker's answer to a subscriber that was not there yet, and a share
/// group is not such a subscriber: the protocol sends no retained message to one. The plain
/// subscription that follows is what proves the broker was holding one all along.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_retained_message_does_not_reach_a_shared_subscription() {
    let Some(url) = test_url() else { return };
    let topic = unique("retained-shared");

    let sender = connect(&url, "retained-shared-sender").await;
    let publisher = sender.publisher();
    publisher
        .message(&State::new(b"online"))
        .to(&topic)
        .retain(true)
        .publish()
        .await
        .expect("publish succeeds");

    let group = connect(&url, "retained-shared-group").await;
    let mut shared = group
        .subscribe_topic(MqttTopic::new(&topic).shared("workers"))
        .await
        .expect("the group member subscribes");
    {
        let mut stream = pin!(shared.stream());
        assert!(
            tokio::time::timeout(QUIET, stream.next()).await.is_err(),
            "a share group receives no retained message"
        );
    }

    let plain = connect(&url, "retained-shared-plain").await;
    let mut ordinary = plain
        .subscribe_topic(MqttTopic::new(&topic))
        .await
        .expect("the plain consumer subscribes");
    {
        let mut stream = pin!(ordinary.stream());
        let message = tokio::time::timeout(RECV_TIMEOUT, stream.next())
            .await
            .expect("the retained message arrives on a subscription that takes one")
            .expect("stream is open")
            .expect("delivery is ok");
        assert_eq!(message.payload(), b"online");
        message.ack().await.expect("ack succeeds");
    }

    // The group reads what is published from now on, which is what says it was subscribed and
    // not merely silent.
    publisher
        .message(&State::new(b"live"))
        .to(&topic)
        .publish()
        .await
        .expect("publish succeeds");
    {
        let mut stream = pin!(shared.stream());
        let message = tokio::time::timeout(RECV_TIMEOUT, stream.next())
            .await
            .expect("the group reads a live publish")
            .expect("stream is open")
            .expect("delivery is ok");
        assert_eq!(message.payload(), b"live");
        message.ack().await.expect("ack succeeds");
    }

    publisher
        .message(&State::new(b""))
        .to(&topic)
        .retain(true)
        .publish()
        .await
        .expect("the retained message is cleared");
    sender.shutdown().await.expect("shutdown succeeds");
    group.shutdown().await.expect("shutdown succeeds");
    plain.shutdown().await.expect("shutdown succeeds");
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

/// A client of the broker's own, subscribed and past its `SUBACK`, so what it reads afterwards is
/// the packet this crate produced rather than this crate's own view of it.
async fn raw_subscriber(url: &str, id: &str, topic: &str) -> (AsyncClient, EventLoop) {
    let authority = ServerSpec::host_from_url(url);
    let (host, port) = authority
        .rsplit_once(':')
        .expect("the stand url names a port");

    let mut options = MqttOptions::new(
        format!("{id}-{}", std::process::id()),
        host,
        port.parse::<u16>().expect("the port parses"),
    );
    options.set_max_packet_size(Some(1024 * 1024));
    let (client, mut eventloop) = AsyncClient::new(options, 16);
    client
        .subscribe(topic.to_owned(), QoS::AtLeastOnce)
        .await
        .expect("the raw client subscribes");
    // Poll until the SUBACK, so a publish cannot race the subscription.
    loop {
        let event = tokio::time::timeout(RECV_TIMEOUT, eventloop.poll())
            .await
            .expect("the raw client reaches a SUBACK")
            .expect("the raw event loop stays alive");
        if matches!(event, Event::Incoming(Packet::SubAck(_))) {
            break;
        }
    }
    (client, eventloop)
}

/// The next PUBLISH packet the raw client reads, whatever protocol traffic precedes it.
async fn next_publish(eventloop: &mut EventLoop) -> Publish {
    loop {
        let event = tokio::time::timeout(RECV_TIMEOUT, eventloop.poll())
            .await
            .expect("the raw client receives the publish")
            .expect("the raw event loop stays alive");
        if let Event::Incoming(Packet::Publish(publish)) = event {
            break publish;
        }
    }
}

/// Every header a message carries reaches the packet as the MQTT 5 property it belongs to, read
/// off the wire by a client of the broker's own: the well-known three take the first-class
/// properties and everything else is a user property, so a peer that never heard of this
/// framework reads a plain MQTT message. The payload format indicator travels one way only, so
/// nothing on the crate's receive path could report it back.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_publish_maps_its_headers_onto_the_packets_own_properties() {
    let Some(url) = test_url() else { return };
    let topic = unique("properties");
    let (_raw, mut eventloop) = raw_subscriber(&url, "props", &topic).await;

    let connected = connect(&url, "properties").await;
    let mut headers = HeaderMap::new();
    headers.insert("content-type", "application/json");
    headers.insert("reply-to", "replies/1");
    headers.insert("correlation-id", "corr-1");
    headers.insert("x-tenant", "acme");
    connected
        .publisher()
        .publish(
            OutgoingMessage::new(&topic, br#"{"id":1}"#.as_slice()).with_headers(headers),
            None,
        )
        .await
        .expect("publish succeeds");

    let publish = next_publish(&mut eventloop).await;
    let properties = publish.properties.expect("the packet carries properties");
    assert_eq!(
        properties.payload_format_indicator,
        Some(1),
        "a JSON payload is UTF-8 on the wire"
    );
    assert_eq!(
        properties.content_type.as_deref(),
        Some("application/json"),
        "the media type reaches the packet as the content type property"
    );
    assert_eq!(
        properties.response_topic.as_deref(),
        Some("replies/1"),
        "where an answer goes is the response topic property, not a header of our own"
    );
    assert_eq!(
        properties.correlation_data.as_deref(),
        Some(b"corr-1".as_slice())
    );
    assert_eq!(
        properties.user_properties,
        vec![("x-tenant".to_owned(), "acme".to_owned())],
        "an ordinary header is a user property and nothing else is"
    );

    // The mirror: a media type that is not textual declares the protocol's other answer, and a
    // message carrying no headers declares nothing at all.
    let mut binary = HeaderMap::new();
    binary.insert("content-type", "application/octet-stream");
    connected
        .publisher()
        .publish(
            OutgoingMessage::new(&topic, b"\x00\x01".as_slice()).with_headers(binary),
            None,
        )
        .await
        .expect("publish succeeds");
    let publish = next_publish(&mut eventloop).await;
    let properties = publish.properties.expect("the packet carries properties");
    assert_eq!(
        properties.payload_format_indicator,
        Some(0),
        "unspecified bytes is what the protocol says about a binary media type"
    );
    assert_eq!(properties.response_topic, None);
    assert!(properties.user_properties.is_empty());

    connected
        .publisher()
        .publish(OutgoingMessage::new(&topic, b"plain".as_slice()), None)
        .await
        .expect("publish succeeds");
    let publish = next_publish(&mut eventloop).await;
    assert_eq!(
        publish
            .properties
            .and_then(|properties| properties.payload_format_indicator),
        None,
        "a message with no headers takes the protocol's own defaults"
    );

    connected.shutdown().await.expect("shutdown succeeds");
}

/// What an acknowledgement is worth, read off the broker rather than off this client: the only
/// place MQTT records one is the session, so the proof is what a resumed session hands back.
///
/// The first leg leaves the delivery unacknowledged, which is what `HandlerOutcome::retry()`
/// settles to here, and the resumed session hands it back. The second leg acknowledges it, and
/// the session hands back nothing - a sentinel published afterwards is what tells silence from
/// slowness. A returning delivery also reports no attempt number, which is why the framework's
/// retry cap counts on a header of its own.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_delivery_returns_on_resume_until_it_is_acknowledged() {
    let Some(url) = test_url() else { return };

    let client_id = format!("it-unacked-{}", std::process::id());
    let topic = unique("unacked");
    let resume = async |url: String, client_id: String| {
        MqttBroker::new(url, client_id)
            .clean_start(false)
            .session_expiry(Duration::from_secs(300))
            .connect()
            .await
            .expect("the session resumes")
    };

    let first = resume(url.clone(), client_id.clone()).await;
    let mut subscriber = first
        .subscribe_topic(MqttTopic::new(&topic).qos(Qos::AtLeastOnce))
        .await
        .expect("subscription opens");
    first
        .publisher()
        .publish(OutgoingMessage::new(&topic, b"once".as_slice()), None)
        .await
        .expect("publish succeeds");
    {
        let mut stream = pin!(subscriber.stream());
        let message = tokio::time::timeout(RECV_TIMEOUT, stream.next())
            .await
            .expect("delivery arrives")
            .expect("stream is open")
            .expect("delivery is ok");
        assert_eq!(message.payload(), b"once");
        // Never settled, the way a refused negative acknowledgement leaves a delivery.
        drop(message);
    }
    first.shutdown().await.expect("shutdown succeeds");

    let second = resume(url.clone(), client_id.clone()).await;
    let mut resumed = second
        .subscribe_topic(MqttTopic::new(&topic).qos(Qos::AtLeastOnce))
        .await
        .expect("subscription reopens");
    {
        let mut stream = pin!(resumed.stream());
        let message = tokio::time::timeout(RECV_TIMEOUT, stream.next())
            .await
            .expect("the unacknowledged delivery comes back on resume")
            .expect("stream is open")
            .expect("delivery is ok");
        assert_eq!(message.payload(), b"once");
        assert_eq!(
            message.redelivery_count(),
            None,
            "MQTT carries a duplicate flag and no counter, so nothing here reports an attempt"
        );
        message.ack().await.expect("ack succeeds");
    }
    second.shutdown().await.expect("shutdown succeeds");

    let third = resume(url.clone(), client_id).await;
    let mut settled = third
        .subscribe_topic(MqttTopic::new(&topic).qos(Qos::AtLeastOnce))
        .await
        .expect("subscription reopens");
    // A publish that did happen is what tells an acknowledged delivery from a slow one: whatever
    // arrives first is either the message the broker still holds or this one.
    third
        .publisher()
        .publish(OutgoingMessage::new(&topic, b"sentinel".as_slice()), None)
        .await
        .expect("publish succeeds");
    {
        let mut stream = pin!(settled.stream());
        let message = tokio::time::timeout(RECV_TIMEOUT, stream.next())
            .await
            .expect("delivery arrives")
            .expect("stream is open")
            .expect("delivery is ok");
        assert_eq!(
            message.payload(),
            b"sentinel",
            "the acknowledgement reached the broker, so the session held nothing"
        );
        message.ack().await.expect("ack succeeds");
    }
    third.shutdown().await.expect("shutdown succeeds");
}

/// How long an assertion of absence waits before it counts as absence. Every such assertion is
/// paired with a delivery that does arrive, so what is being timed is a broker that already had
/// its answer ready rather than a slow network.
const QUIET: Duration = Duration::from_millis(500);

/// The announced maximum packet size is the broker's instruction, not this client's own buffer:
/// a payload above it is never sent. The crate announces 1 MiB, which is what carries a real
/// payload past the client library's own 10 KiB default, and a connection that announces less
/// misses the same message while still receiving a small one.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_announced_maximum_packet_size_decides_what_the_broker_sends() {
    let Some(url) = test_url() else { return };

    let topic = unique("large");
    let roomy = connect(&url, "roomy").await;
    let cramped = MqttBroker::new(&url, format!("it-cramped-{}", std::process::id()))
        .max_packet_size(1024)
        .connect()
        .await
        .expect("broker connects");

    let mut wide = roomy
        .subscribe_topic(MqttTopic::new(&topic))
        .await
        .expect("subscription opens");
    let mut narrow = cramped
        .subscribe_topic(MqttTopic::new(&topic))
        .await
        .expect("subscription opens");

    let payload = vec![b'x'; 64 * 1024];
    let publisher = roomy.publisher();
    publisher
        .publish(OutgoingMessage::new(&topic, payload.as_slice()), None)
        .await
        .expect("publish succeeds");
    // Small enough for both, so the capped subscription proves it is live rather than stalled.
    publisher
        .publish(OutgoingMessage::new(&topic, b"small".as_slice()), None)
        .await
        .expect("publish succeeds");

    let mut wide_stream = pin!(wide.stream());
    let message = tokio::time::timeout(RECV_TIMEOUT, wide_stream.next())
        .await
        .expect("delivery arrives")
        .expect("stream is open")
        .expect("delivery is ok");
    assert_eq!(
        message.payload().len(),
        payload.len(),
        "the crate's own maximum carries a payload the client library's default would refuse"
    );
    message.ack().await.expect("ack succeeds");

    let mut narrow_stream = pin!(narrow.stream());
    let message = tokio::time::timeout(RECV_TIMEOUT, narrow_stream.next())
        .await
        .expect("delivery arrives")
        .expect("stream is open")
        .expect("delivery is ok");
    assert_eq!(
        message.payload(),
        b"small",
        "the broker skipped the packet that did not fit what this connection announced"
    );
    message.ack().await.expect("ack succeeds");

    roomy.shutdown().await.expect("shutdown succeeds");
    cramped.shutdown().await.expect("shutdown succeeds");
}

/// Flow control is the broker's counter, not this client's queue: with a receive maximum of one,
/// a second delivery waits at the broker until the first is acknowledged. That is what bounds an
/// unread subscriber's queue, and it is visible only where a real broker is counting.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_receive_maximum_holds_deliveries_at_the_broker_until_one_is_settled() {
    let Some(url) = test_url() else { return };

    let topic = unique("inflight");
    let consumer = MqttBroker::new(&url, format!("it-inflight-{}", std::process::id()))
        .receive_maximum(1)
        .connect()
        .await
        .expect("broker connects");
    let mut subscriber = consumer
        .subscribe_topic(MqttTopic::new(&topic).qos(Qos::AtLeastOnce))
        .await
        .expect("subscription opens");

    let sender = connect(&url, "inflight-sender").await;
    let publisher = sender.publisher();
    for payload in [b"first".as_slice(), b"second".as_slice()] {
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
    assert_eq!(first.payload(), b"first");

    assert!(
        tokio::time::timeout(QUIET, stream.next()).await.is_err(),
        "one delivery is in flight, so the broker is holding the second"
    );

    first.ack().await.expect("ack succeeds");
    let second = tokio::time::timeout(RECV_TIMEOUT, stream.next())
        .await
        .expect("the acknowledgement released the next delivery")
        .expect("stream is open")
        .expect("delivery is ok");
    assert_eq!(second.payload(), b"second");
    second.ack().await.expect("ack succeeds");

    consumer.shutdown().await.expect("shutdown succeeds");
    sender.shutdown().await.expect("shutdown succeeds");
}

/// Clearing a retained topic is a retained publish with an empty payload, and only the broker can
/// say it worked: a subscriber arriving afterwards must find nothing kept for it.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_empty_retained_payload_clears_the_topic() {
    let Some(url) = test_url() else { return };
    let topic = unique("cleared");

    let sender = connect(&url, "cleared-sender").await;
    let publisher = sender.publisher();
    publisher
        .message(&State::new(b"online"))
        .to(&topic)
        .retain(true)
        .publish()
        .await
        .expect("publish succeeds");

    let early = connect(&url, "cleared-early").await;
    let mut kept = early
        .subscribe_topic(MqttTopic::new(&topic))
        .await
        .expect("subscription opens");
    {
        let mut stream = pin!(kept.stream());
        let message = tokio::time::timeout(RECV_TIMEOUT, stream.next())
            .await
            .expect("the retained message arrives on subscribe")
            .expect("stream is open")
            .expect("delivery is ok");
        assert_eq!(message.payload(), b"online");
        message.ack().await.expect("ack succeeds");
    }

    publisher
        .message(&State::new(b""))
        .to(&topic)
        .retain(true)
        .publish()
        .await
        .expect("the retained message is cleared");

    let late = connect(&url, "cleared-late").await;
    let mut nothing_kept = late
        .subscribe_topic(MqttTopic::new(&topic))
        .await
        .expect("subscription opens");
    // A publish that did happen afterwards is what tells a cleared topic from a slow one.
    publisher
        .message(&State::new(b"live"))
        .to(&topic)
        .publish()
        .await
        .expect("publish succeeds");
    {
        let mut stream = pin!(nothing_kept.stream());
        let message = tokio::time::timeout(RECV_TIMEOUT, stream.next())
            .await
            .expect("delivery arrives")
            .expect("stream is open")
            .expect("delivery is ok");
        assert_eq!(
            message.payload(),
            b"live",
            "the broker kept nothing, so the first delivery is the one published after the subscribe"
        );
        message.ack().await.expect("ack succeeds");
    }

    sender.shutdown().await.expect("shutdown succeeds");
    early.shutdown().await.expect("shutdown succeeds");
    late.shutdown().await.expect("shutdown succeeds");
}

/// A request names where its answer goes and what to match it against, and both ride first-class
/// MQTT 5 properties rather than an envelope: a responder reads them as the `reply-to` and
/// `correlation-id` headers and answers on the topic it was handed. The plain message that
/// follows carries neither, which is how a responder tells a request from an announcement.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_request_carries_where_its_answer_goes_and_what_matches_it() {
    let Some(url) = test_url() else { return };

    let requests = unique("requests");
    let replies = unique("replies");
    let responder = connect(&url, "responder").await;
    let requester = connect(&url, "requester").await;

    let mut inbox = responder
        .subscribe_topic(MqttTopic::new(&requests).qos(Qos::AtLeastOnce))
        .await
        .expect("the responder subscribes");
    let mut answers = requester
        .subscribe_topic(MqttTopic::new(&replies).qos(Qos::AtLeastOnce))
        .await
        .expect("the requester subscribes for its answer");

    let mut headers = HeaderMap::new();
    headers.insert("reply-to", replies.clone());
    headers.insert("correlation-id", "corr-7");
    requester
        .publisher()
        .publish(
            OutgoingMessage::new(&requests, b"ping".as_slice()).with_headers(headers),
            None,
        )
        .await
        .expect("publish succeeds");
    requester
        .publisher()
        .publish(
            OutgoingMessage::new(&requests, b"announcement".as_slice()),
            None,
        )
        .await
        .expect("publish succeeds");

    let mut inbox_stream = pin!(inbox.stream());
    let request = tokio::time::timeout(RECV_TIMEOUT, inbox_stream.next())
        .await
        .expect("the request arrives")
        .expect("stream is open")
        .expect("delivery is ok");
    let reply_to = request
        .headers()
        .reply_to()
        .expect("the request names where its answer goes")
        .to_owned();
    let correlation = request
        .headers()
        .correlation_id()
        .expect("the request names what matches its answer")
        .to_owned();
    assert_eq!(reply_to, replies);
    assert_eq!(correlation, "corr-7");
    request.ack().await.expect("ack succeeds");

    let plain = tokio::time::timeout(RECV_TIMEOUT, inbox_stream.next())
        .await
        .expect("the announcement arrives")
        .expect("stream is open")
        .expect("delivery is ok");
    assert_eq!(plain.headers().reply_to(), None);
    assert_eq!(plain.headers().correlation_id(), None);
    plain.ack().await.expect("ack succeeds");

    let mut answer_headers = HeaderMap::new();
    answer_headers.insert("correlation-id", correlation.clone());
    responder
        .publisher()
        .publish(
            OutgoingMessage::new(&reply_to, b"pong".as_slice()).with_headers(answer_headers),
            None,
        )
        .await
        .expect("publish succeeds");

    let mut answer_stream = pin!(answers.stream());
    let answer = tokio::time::timeout(RECV_TIMEOUT, answer_stream.next())
        .await
        .expect("the answer arrives on the topic the request named")
        .expect("stream is open")
        .expect("delivery is ok");
    assert_eq!(answer.payload(), b"pong");
    assert_eq!(answer.headers().correlation_id(), Some("corr-7"));
    answer.ack().await.expect("ack succeeds");

    responder.shutdown().await.expect("shutdown succeeds");
    requester.shutdown().await.expect("shutdown succeeds");
}

/// Connects a client of the broker's own under `client_id`, which takes the session away from
/// whoever held it: the broker ends the older connection, and that is the one disconnection a
/// test can ask for without touching the stand.
async fn take_over(url: &str, client_id: &str) -> (AsyncClient, EventLoop) {
    let authority = ServerSpec::host_from_url(url);
    let (host, port) = authority
        .rsplit_once(':')
        .expect("the stand url names a port");

    let mut options = MqttOptions::new(
        client_id.to_owned(),
        host,
        port.parse::<u16>().expect("the port parses"),
    );
    options.set_clean_start(true);
    let (client, mut eventloop) = AsyncClient::new(options, 16);
    loop {
        let event = tokio::time::timeout(RECV_TIMEOUT, eventloop.poll())
            .await
            .expect("the taking client reaches a CONNACK")
            .expect("the taking event loop stays alive");
        if matches!(event, Event::Incoming(Packet::ConnAck(_))) {
            break;
        }
    }
    (client, eventloop)
}

/// The last will is the broker's message, not this client's: it is published exactly when the
/// session ends without a `DISCONNECT`, which is what a service uses to announce that it died.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_broker_publishes_the_last_will_when_a_session_ends_abruptly() {
    let Some(url) = test_url() else { return };
    let will_topic = unique("status");

    let watcher = connect(&url, "will-watcher").await;
    let mut watching = watcher
        .subscribe_topic(MqttTopic::new(&will_topic).qos(Qos::AtLeastOnce))
        .await
        .expect("subscription opens");

    let client_id = format!("it-will-{}", std::process::id());
    let doomed = MqttBroker::new(&url, client_id.clone())
        .last_will(&will_topic, b"offline".to_vec(), Qos::AtLeastOnce, false)
        .connect()
        .await
        .expect("broker connects");

    let (_taker, _taker_loop) = take_over(&url, &client_id).await;

    let mut stream = pin!(watching.stream());
    let message = tokio::time::timeout(RECV_TIMEOUT, stream.next())
        .await
        .expect("the broker publishes the will of the session it ended")
        .expect("stream is open")
        .expect("delivery is ok");
    assert_eq!(message.payload(), b"offline");
    message.ack().await.expect("ack succeeds");

    let _ = doomed.shutdown().await;
    watcher.shutdown().await.expect("shutdown succeeds");
}

/// The mirror, and the reason `shutdown` sends a `DISCONNECT` at all: a service that ends its own
/// session announces nothing, so the will stays where it was declared.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_clean_shutdown_publishes_no_last_will() {
    let Some(url) = test_url() else { return };
    let will_topic = unique("clean-status");

    let watcher = connect(&url, "clean-will-watcher").await;
    let mut watching = watcher
        .subscribe_topic(MqttTopic::new(&will_topic).qos(Qos::AtLeastOnce))
        .await
        .expect("subscription opens");

    let departing = MqttBroker::new(&url, format!("it-clean-will-{}", std::process::id()))
        .last_will(&will_topic, b"offline".to_vec(), Qos::AtLeastOnce, false)
        .connect()
        .await
        .expect("broker connects");
    departing.shutdown().await.expect("shutdown succeeds");

    // A publish that did happen is what tells a suppressed will from a slow one.
    watcher
        .publisher()
        .publish(
            OutgoingMessage::new(&will_topic, b"sentinel".as_slice()),
            None,
        )
        .await
        .expect("publish succeeds");

    let mut stream = pin!(watching.stream());
    let message = tokio::time::timeout(RECV_TIMEOUT, stream.next())
        .await
        .expect("delivery arrives")
        .expect("stream is open")
        .expect("delivery is ok");
    assert_eq!(
        message.payload(),
        b"sentinel",
        "the session ended with a DISCONNECT, so the broker published no will"
    );
    message.ack().await.expect("ack succeeds");

    watcher.shutdown().await.expect("shutdown succeeds");
}

/// Recovery is the connection task's own business: a session taken away by another client ends
/// the connection, and the task reconnects, sees the broker report the session gone, and
/// subscribes its filters again. A service notices nothing but the gap.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_subscription_comes_back_after_the_connection_is_lost() {
    let Some(url) = test_url() else { return };
    let topic = unique("resubscribe");

    let client_id = format!("it-resub-{}", std::process::id());
    let held = MqttBroker::new(&url, client_id.clone())
        .connect()
        .await
        .expect("broker connects");
    let mut subscriber = held
        .subscribe_topic(MqttTopic::new(&topic).qos(Qos::AtLeastOnce))
        .await
        .expect("subscription opens");

    let sender = connect(&url, "resub-sender").await;
    let publisher = sender.publisher();
    publisher
        .publish(OutgoingMessage::new(&topic, b"before".as_slice()), None)
        .await
        .expect("publish succeeds");
    let mut stream = pin!(subscriber.stream());
    let message = tokio::time::timeout(RECV_TIMEOUT, stream.next())
        .await
        .expect("delivery arrives")
        .expect("stream is open")
        .expect("delivery is ok");
    assert_eq!(message.payload(), b"before");
    message.ack().await.expect("ack succeeds");

    // The taking client starts a clean session under the same id, so the subscription is gone
    // from the broker along with the connection that held it.
    let (_taker, _taker_loop) = take_over(&url, &client_id).await;

    // Nothing published during the gap can be delivered, so the publish is what the poll is made
    // of: the first one that lands is the one that arrived after the filter was back.
    let deadline = tokio::time::Instant::now() + RECV_TIMEOUT;
    let message = loop {
        publisher
            .publish(OutgoingMessage::new(&topic, b"after".as_slice()), None)
            .await
            .expect("publish succeeds");
        if let Ok(Some(delivery)) =
            tokio::time::timeout(Duration::from_millis(250), stream.next()).await
        {
            break delivery.expect("delivery is ok");
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "the connection task never brought the subscription back"
        );
    };
    assert_eq!(message.payload(), b"after");
    message.ack().await.expect("ack succeeds");

    held.shutdown().await.expect("shutdown succeeds");
    sender.shutdown().await.expect("shutdown succeeds");
}

/// The delivery's quality of service is the lower of the two sides, so the publisher decides what
/// a subscription can do with a message: asking for the strongest guarantee on the subscribe side
/// buys nothing over a publisher that sends fire and forget. The mirror on the same subscription
/// is what makes the refusal the publisher's doing rather than the subscription's.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_fire_and_forget_publish_leaves_the_strongest_subscription_nothing_to_settle() {
    let Some(url) = test_url() else { return };
    let connected = connect(&url, "qos-cap").await;

    let topic = unique("capped-qos");
    let mut subscriber = connected
        .subscribe_topic(MqttTopic::new(&topic).qos(Qos::ExactlyOnce))
        .await
        .expect("subscription opens");

    let publisher = connected.publisher();
    publisher
        .message(&State::new(b"fire"))
        .to(&topic)
        .qos(Qos::AtMostOnce)
        .publish()
        .await
        .expect("publish succeeds");

    let mut stream = pin!(subscriber.stream());
    let message = tokio::time::timeout(RECV_TIMEOUT, stream.next())
        .await
        .expect("delivery arrives")
        .expect("stream is open")
        .expect("delivery is ok");
    assert_eq!(message.payload(), b"fire");
    assert!(
        matches!(message.ack().await, Err(AckError::Unsupported)),
        "the publisher sent at QoS 0, so this delivery carries no acknowledgement"
    );

    publisher
        .message(&State::new(b"handshake"))
        .to(&topic)
        .qos(Qos::ExactlyOnce)
        .publish()
        .await
        .expect("publish succeeds");
    let message = tokio::time::timeout(RECV_TIMEOUT, stream.next())
        .await
        .expect("delivery arrives")
        .expect("stream is open")
        .expect("delivery is ok");
    assert_eq!(message.payload(), b"handshake");
    message
        .ack()
        .await
        .expect("the same subscription settles what the publisher sent at QoS 2");

    connected.shutdown().await.expect("shutdown succeeds");
}

/// The URL of the listener that requires a user name and a password, or `None` when there is no
/// stand. It is a second listener of the same broker, because authentication is the one capability
/// an anonymous stand cannot answer for.
fn auth_url() -> Option<String> {
    match std::env::var("MQTT_TEST_AUTH_URL") {
        Ok(url) if !url.is_empty() => Some(url),
        _ => {
            assert!(
                std::env::var_os("RUSTSTREAM_REQUIRE_LIVE").is_none(),
                "RUSTSTREAM_REQUIRE_LIVE is set, so the credentials test must run, \
                 but MQTT_TEST_AUTH_URL is missing or empty"
            );
            eprintln!("MQTT_TEST_AUTH_URL is not set; skipping the credentials test");
            None
        }
    }
}

/// Credentials are the broker's to accept, so the proof is a session that carries a message:
/// a `CONNACK` on its own says the packet was well formed, not that the user exists.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_broker_that_demands_credentials_accepts_the_ones_the_builder_records() {
    let Some(url) = auth_url() else { return };

    let connected = MqttBroker::new(&url, format!("it-auth-{}", std::process::id()))
        .credentials("tester", "s3cret")
        .connect()
        .await
        .expect("the broker accepts the credentials");

    let topic = unique("authenticated");
    let mut subscriber = connected
        .subscribe_topic(MqttTopic::new(&topic).qos(Qos::AtLeastOnce))
        .await
        .expect("subscription opens");
    connected
        .publisher()
        .publish(OutgoingMessage::new(&topic, b"hello".as_slice()), None)
        .await
        .expect("publish succeeds");

    let mut stream = pin!(subscriber.stream());
    let message = tokio::time::timeout(RECV_TIMEOUT, stream.next())
        .await
        .expect("delivery arrives")
        .expect("stream is open")
        .expect("delivery is ok");
    assert_eq!(message.payload(), b"hello");
    message.ack().await.expect("ack succeeds");

    connected.shutdown().await.expect("shutdown succeeds");
}

/// The refusal a service meets when the credentials are wrong or absent: `connect` reports it
/// instead of retrying forever, because no amount of retrying turns a bad password into a good
/// one. This is what tells a misconfigured deployment from an unreachable broker.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_broker_that_demands_credentials_refuses_the_wrong_ones() {
    let Some(url) = auth_url() else { return };

    let wrong = MqttBroker::new(&url, format!("it-auth-wrong-{}", std::process::id()))
        .credentials("tester", "not-the-password");
    let absent = MqttBroker::new(&url, format!("it-auth-absent-{}", std::process::id()));

    for (broker, what) in [
        (wrong, "a wrong password"),
        (absent, "no credentials at all"),
    ] {
        let error = tokio::time::timeout(RECV_TIMEOUT, broker.connect())
            .await
            .unwrap_or_else(|_| panic!("connect reports {what} instead of retrying"))
            .expect_err("the broker refuses the session");
        assert!(
            matches!(error, MqttError::Connect(_)),
            "{what} is a connection refusal: {error}"
        );
    }
}

/// The TLS listener's URL and the directory the stand's certificate chain was generated into, or
/// `None` when there is no stand.
fn tls_stand() -> Option<(String, PathBuf)> {
    match (
        std::env::var("MQTT_TEST_TLS_URL"),
        std::env::var("MQTT_TEST_TLS_DIR"),
    ) {
        (Ok(url), Ok(dir)) if !url.is_empty() && !dir.is_empty() => Some((url, PathBuf::from(dir))),
        _ => {
            assert!(
                std::env::var_os("RUSTSTREAM_REQUIRE_LIVE").is_none(),
                "RUSTSTREAM_REQUIRE_LIVE is set, so the TLS tests must run, but \
                 MQTT_TEST_TLS_URL or MQTT_TEST_TLS_DIR is missing or empty"
            );
            eprintln!("MQTT_TEST_TLS_URL is not set; skipping the TLS test");
            None
        }
    }
}

/// One of the stand's generated PEM files.
fn pem(dir: &Path, name: &str) -> Vec<u8> {
    std::fs::read(dir.join(name))
        .unwrap_or_else(|err| panic!("the stand's {name} is readable: {err}"))
}

/// Both halves of the crate's TLS surface at once: the listener verifies the client against the
/// stand's authority and the client verifies the listener against the same one, so a session that
/// carries a message says each PEM reached the place it belongs.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_tls_listener_accepts_the_certificates_the_builder_records() {
    let Some((url, dir)) = tls_stand() else {
        return;
    };

    let connected = MqttBroker::new(&url, format!("it-tls-{}", std::process::id()))
        .tls_ca(pem(&dir, "ca.crt"))
        .tls_client_auth(pem(&dir, "client.crt"), pem(&dir, "client.key"))
        .connect()
        .await
        .expect("the TLS session is established");

    let topic = unique("over-tls");
    let mut subscriber = connected
        .subscribe_topic(MqttTopic::new(&topic).qos(Qos::AtLeastOnce))
        .await
        .expect("subscription opens");
    connected
        .publisher()
        .publish(OutgoingMessage::new(&topic, b"encrypted".as_slice()), None)
        .await
        .expect("publish succeeds");

    let mut stream = pin!(subscriber.stream());
    let message = tokio::time::timeout(RECV_TIMEOUT, stream.next())
        .await
        .expect("delivery arrives")
        .expect("stream is open")
        .expect("delivery is ok");
    assert_eq!(message.payload(), b"encrypted");
    message.ack().await.expect("ack succeeds");

    connected.shutdown().await.expect("shutdown succeeds");
}

/// An authority that did not sign the server's certificate verifies nothing, and that answer is
/// the same on every attempt: `connect` reports the handshake rather than retrying it until the
/// wait turns into a timeout naming nothing. This is what a wrong `tls_ca` looks like in a
/// deployment - a real certificate, just not the one that signed the server's.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_certificate_authority_that_signed_nothing_is_reported_rather_than_retried() {
    let Some((url, dir)) = tls_stand() else {
        return;
    };

    let error = tokio::time::timeout(
        RECV_TIMEOUT,
        MqttBroker::new(&url, format!("it-tls-wrong-ca-{}", std::process::id()))
            .tls_ca(pem(&dir, "client.crt"))
            .tls_client_auth(pem(&dir, "client.crt"), pem(&dir, "client.key"))
            .connect(),
    )
    .await
    .expect("the refusal is reported instead of being retried")
    .expect_err("the server's certificate was signed by an authority this client does not hold");

    let reported = error.to_string();
    assert!(
        matches!(error, MqttError::Connect(_)) && reported.contains("tls handshake failed"),
        "the error names the handshake that failed: {reported}"
    );
}
