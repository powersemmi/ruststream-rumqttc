//! The in-process mode answering the way a server does, on the production broker connected with
//! `connect_in_process`.
//!
//! Most tests here are the twin of one in `integration_mqtt.rs`, written in the same shape against
//! the in-process transport instead of Mosquitto: a service reading an in-process assertion must be
//! able to find the live one that backs it. The subject is the transport, so these drive the
//! connected form directly; a service's tests drive its app through `TestApp`.
//!
//! Each broker connected in process is a server of its own, so what several connections share on
//! one server (competing consumers across processes, a persistent session that outlives its
//! connection) is the live suite's alone.

#![cfg(feature = "testing")]

use std::pin::pin;
use std::time::Duration;

use futures::StreamExt;
use ruststream::testing::{InProcess, TestableBroker};
use ruststream::{
    AckError, ConnectedBroker, HeaderMap, IncomingMessage, OutgoingMessage, Publisher, Subscriber,
};
use ruststream_rumqttc::{
    ConnectedMqttBroker, MqttBroker, MqttError, MqttFilter, MqttMessage, MqttPublishOptions,
    MqttSubscriber, MqttTopic, Qos,
};

/// Long enough that a delivery which is coming has arrived; the transport is a channel, so
/// nothing here waits on a network.
const SETTLE: Duration = Duration::from_millis(100);

/// The one per-message argument these tests take: publish at `qos`, whatever the policy declares.
fn at(qos: Qos) -> MqttPublishOptions {
    MqttPublishOptions::default().qos(qos)
}

fn broker() -> MqttBroker {
    MqttBroker::new("mqtt://localhost:1883", "in-process")
}

async fn connected() -> ConnectedMqttBroker {
    broker()
        .connect_in_process()
        .await
        .expect("the production broker connects in process")
}

async fn publish(connected: &ConnectedMqttBroker, topic: &str, payload: &[u8]) {
    connected
        .publisher()
        .publish(OutgoingMessage::new(topic, payload), None)
        .await
        .expect("publish succeeds");
}

/// The next delivery on `subscriber`, or `None` once nothing more arrives.
async fn next(subscriber: &mut MqttSubscriber) -> Option<MqttMessage> {
    let mut stream = pin!(subscriber.stream());
    tokio::time::timeout(SETTLE, stream.next())
        .await
        .ok()
        .map(|delivery| delivery.expect("stream is open").expect("delivery is ok"))
}

/// A connection that is not configured the way `connect` would accept is not one a test can
/// connect either: the in-process mode builds the same options first.
#[tokio::test]
async fn a_configuration_connect_refuses_is_refused_in_process() {
    let error = broker()
        .keep_alive(Duration::from_secs(1))
        .connect_in_process()
        .await
        .expect_err("the protocol floor is five seconds");
    assert!(
        matches!(&error, MqttError::Invalid(reason) if reason.contains("5 seconds")),
        "{error}"
    );

    let error = MqttBroker::new("mqtts://localhost:8883", "tls")
        .connect_in_process()
        .await
        .expect_err("nothing can verify the server");
    assert!(
        matches!(&error, MqttError::Invalid(reason) if reason.contains("tls_ca")),
        "{error}"
    );
}

/// Two members of one group on one connection are one subscription to the server, so they take
/// turns, and four publishes are four deliveries across the group.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_share_group_takes_one_copy_between_its_members() {
    let connected = connected().await;
    let mut first = connected
        .subscribe_topic(MqttTopic::new("jobs").shared("workers"))
        .await
        .expect("first member subscribes");
    let mut second = connected
        .subscribe_topic(MqttTopic::new("jobs").shared("workers"))
        .await
        .expect("second member subscribes");

    for i in 0..4u8 {
        publish(&connected, "jobs", &[i]).await;
    }

    let mut seen = Vec::new();
    for subscriber in [&mut first, &mut second] {
        while let Some(message) = next(subscriber).await {
            seen.push(message.payload()[0]);
            message.ack().await.expect("ack succeeds");
        }
    }
    seen.sort_unstable();
    assert_eq!(
        seen,
        vec![0, 1, 2, 3],
        "one delivery per publish, not one per member"
    );

    connected.shutdown().await.expect("shutdown succeeds");
}

/// One filter subscribed twice on one connection is one subscription to the server as well, so
/// the two take turns rather than each receiving a copy.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn one_filter_subscribed_twice_takes_one_copy_between_them() {
    let connected = connected().await;
    let mut first = connected
        .subscribe_topic(MqttTopic::new("jobs"))
        .await
        .expect("first subscribes");
    let mut second = connected
        .subscribe_topic(MqttTopic::new("jobs"))
        .await
        .expect("second subscribes");

    for i in 0..2u8 {
        publish(&connected, "jobs", &[i]).await;
    }

    let mut delivered = 0;
    for subscriber in [&mut first, &mut second] {
        while next(subscriber).await.is_some() {
            delivered += 1;
        }
    }
    assert_eq!(
        delivered, 2,
        "two publishes on one wire filter are two deliveries"
    );

    connected.shutdown().await.expect("shutdown succeeds");
}

/// Two filters overlapping on one topic are two subscriptions to the server, and a server sends a
/// packet for each, naming the subscription it is for. Each filter receives its own packet once,
/// and each delivery acknowledges it.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn overlapping_filters_each_acknowledge_their_own_packet() {
    let connected = connected().await;
    let mut narrow = connected
        .subscribe_filter(MqttFilter::new("devices/+/state"))
        .await
        .expect("narrow filter subscribes");
    let mut wide = connected
        .subscribe_filter(MqttFilter::new("devices/#"))
        .await
        .expect("wide filter subscribes");

    publish(&connected, "devices/dev42/state", b"on").await;

    for subscriber in [&mut narrow, &mut wide] {
        let mut acknowledged = 0;
        while let Some(message) = next(subscriber).await {
            match message.ack().await {
                Ok(()) => acknowledged += 1,
                Err(other) => panic!("unexpected settlement: {other}"),
            }
        }
        assert_eq!(
            acknowledged, 1,
            "each filter receives the packet sent for it once, and acknowledges it"
        );
    }

    connected.shutdown().await.expect("shutdown succeeds");
}

/// The twin of `a_retained_message_does_not_reach_a_shared_subscription`: the server keeps the
/// last retained message of a topic and hands it to a plain subscription that arrives later, not
/// to a share group; an empty retained payload clears it.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_retained_message_reaches_a_later_plain_subscription_only() {
    let connected = connected().await;
    let retained = MqttPublishOptions::default().retain(true);
    connected
        .publisher()
        .publish(
            OutgoingMessage::new("devices/dev42/state", b"online".as_slice()),
            Some(&retained),
        )
        .await
        .expect("publish succeeds");

    let mut group = connected
        .subscribe_filter(MqttFilter::new("devices/+/state").shared("workers"))
        .await
        .expect("the group subscribes");
    assert!(
        next(&mut group).await.is_none(),
        "a share group receives no retained message"
    );

    let mut plain = connected
        .subscribe_filter(MqttFilter::new("devices/+/state"))
        .await
        .expect("the plain filter subscribes");
    let message = next(&mut plain)
        .await
        .expect("the retained message arrives on subscribe");
    assert_eq!(message.payload(), b"online");
    drop(plain);

    connected
        .publisher()
        .publish(
            OutgoingMessage::new("devices/dev42/state", b"".as_slice()),
            Some(&retained),
        )
        .await
        .expect("the clearing publish succeeds");
    let mut later = connected
        .subscribe_filter(MqttFilter::new("devices/+/state"))
        .await
        .expect("a later filter subscribes");
    assert!(
        next(&mut later).await.is_none(),
        "an empty retained payload cleared the topic"
    );

    connected.shutdown().await.expect("shutdown succeeds");
}

/// The session announces its `max_packet_size` when it connects, and a server discards a message
/// larger than that instead of sending it. The limit is the production broker's own setting.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_message_over_the_packet_limit_never_arrives() {
    let connected = broker()
        .max_packet_size(64)
        .connect_in_process()
        .await
        .expect("the production broker connects in process");
    let mut subscriber = connected
        .subscribe_topic(MqttTopic::new("frames"))
        .await
        .expect("subscription opens");

    publish(&connected, "frames", &[0; 8]).await;
    publish(&connected, "frames", &[1; 128]).await;

    let small = next(&mut subscriber)
        .await
        .expect("the small frame arrives");
    assert_eq!(small.payload(), &[0; 8]);
    assert!(
        next(&mut subscriber).await.is_none(),
        "the frame over the limit is discarded"
    );

    connected.shutdown().await.expect("shutdown succeeds");
}

/// The twin of `qos0_reports_ack_unsupported`. A `QoS` 0 delivery carries no acknowledgement on
/// the wire, so it carries none here either.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn qos0_reports_ack_unsupported_in_process() {
    let connected = connected().await;
    let mut subscriber = connected
        .subscribe_topic(MqttTopic::new("fire").qos(Qos::AtMostOnce))
        .await
        .expect("subscription opens");

    publish(&connected, "fire", b"fire").await;

    let message = next(&mut subscriber).await.expect("delivery arrives");
    assert!(matches!(message.ack().await, Err(AckError::Unsupported)));

    connected.shutdown().await.expect("shutdown succeeds");
}

/// A delivery comes out at the lesser of the two levels, so publishing fire-and-forget into an
/// acknowledged subscription is still a delivery that cannot be settled.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_publish_side_qos_caps_the_delivery_in_process() {
    let connected = connected().await;
    let mut subscriber = connected
        .subscribe_topic(MqttTopic::new("mixed").qos(Qos::AtLeastOnce))
        .await
        .expect("subscription opens");

    connected
        .publisher()
        .publish(
            OutgoingMessage::new("mixed", b"fire".as_slice()),
            Some(&at(Qos::AtMostOnce)),
        )
        .await
        .expect("publish succeeds");

    let message = next(&mut subscriber).await.expect("delivery arrives");
    assert!(matches!(message.ack().await, Err(AckError::Unsupported)));

    connected.shutdown().await.expect("shutdown succeeds");
}

/// The twin of `nack_reports_unsupported_and_dropping_acknowledges`: asking for redelivery is
/// refused, declining it acknowledges, and nothing comes back.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn nack_reports_unsupported_and_dropping_acknowledges_in_process() {
    let connected = connected().await;
    let mut subscriber = connected
        .subscribe_topic(MqttTopic::new("orders").qos(Qos::AtLeastOnce))
        .await
        .expect("subscription opens");

    publish(&connected, "orders", b"requeue").await;
    publish(&connected, "orders", b"drop").await;

    let first = next(&mut subscriber).await.expect("delivery arrives");
    assert!(matches!(first.nack(true).await, Err(AckError::Unsupported)));
    let second = next(&mut subscriber).await.expect("delivery arrives");
    second
        .nack(false)
        .await
        .expect("declining redelivery acknowledges");
    assert!(
        next(&mut subscriber).await.is_none(),
        "neither answer brings the message back"
    );

    connected.shutdown().await.expect("shutdown succeeds");
}

/// Headers travel as MQTT 5 properties, both ways, as they do on the wire: the well-known ones
/// ride the first-class properties and every other one a user property.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn headers_travel_as_properties_in_process() {
    let connected = connected().await;
    let mut subscriber = connected
        .subscribe_topic(MqttTopic::new("orders"))
        .await
        .expect("subscription opens");

    let mut headers = HeaderMap::new();
    headers.insert("content-type", "application/json");
    headers.insert("reply-to", "orders/replies");
    headers.insert("correlation-id", "corr-1");
    headers.insert("x-tenant", "acme");
    connected
        .publisher()
        .publish(
            OutgoingMessage::new("orders", b"{}".as_slice()).with_headers(headers),
            None,
        )
        .await
        .expect("publish succeeds");

    let message = next(&mut subscriber).await.expect("delivery arrives");
    let received = message.headers();
    assert_eq!(received.get_str("content-type"), Some("application/json"));
    assert_eq!(received.get_str("reply-to"), Some("orders/replies"));
    assert_eq!(received.get_str("correlation-id"), Some("corr-1"));
    assert_eq!(received.get_str("x-tenant"), Some("acme"));

    connected.shutdown().await.expect("shutdown succeeds");
}

/// A topic no server takes is refused before anything is sent: a wildcard is subscribe-only, and
/// a server answers an empty topic by closing the session.
#[tokio::test]
async fn a_topic_no_server_takes_is_refused() {
    let connected = connected().await;
    for topic in ["", "devices/+/state", "devices/#"] {
        let error = connected
            .publisher()
            .publish(OutgoingMessage::new(topic, b"x".as_slice()), None)
            .await
            .expect_err("the topic is refused");
        assert!(
            matches!(&error, MqttError::Publish { .. }),
            "{topic:?}: {error}"
        );
    }
    connected.shutdown().await.expect("shutdown succeeds");
}

/// A delivery outliving its connection cannot be acknowledged: the session that would take the
/// acknowledgement is gone.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_delivery_cannot_be_acknowledged_after_shutdown() {
    let connected = connected().await;
    let mut subscriber = connected
        .subscribe_topic(MqttTopic::new("orders").qos(Qos::AtLeastOnce))
        .await
        .expect("subscription opens");
    publish(&connected, "orders", b"late").await;
    let message = next(&mut subscriber).await.expect("delivery arrives");

    connected.shutdown().await.expect("shutdown succeeds");

    assert!(
        matches!(message.ack().await, Err(AckError::Broker(_))),
        "the acknowledgement is refused, not reported as done"
    );
}

/// A publisher handed out before the shutdown outlives the connection and reports it.
#[tokio::test]
async fn a_publisher_outliving_the_connection_reports_it() {
    let broker = broker();
    let early = broker.publisher();
    let connected = broker
        .connect_in_process()
        .await
        .expect("the production broker connects in process");
    connected.shutdown().await.expect("shutdown succeeds");

    let error = early
        .publish(OutgoingMessage::new("orders", b"x".as_slice()), None)
        .await
        .expect_err("the connection is gone");
    assert!(matches!(error, MqttError::NotConnected), "{error}");
}

/// Which subscriptions a publish reaches, as the harness asks it in live mode: every filter that
/// matches the topic, and one delivery between the subscriptions this connection opened on one
/// wire filter.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn routes_answers_every_matching_filter_and_one_member_of_a_group() {
    let connected = connected().await;
    let _members = [
        connected
            .subscribe_filter(MqttFilter::new("jobs/+").shared("workers"))
            .await
            .expect("first member subscribes"),
        connected
            .subscribe_filter(MqttFilter::new("jobs/+").shared("workers"))
            .await
            .expect("second member subscribes"),
    ];
    let _wide = connected
        .subscribe_filter(MqttFilter::new("jobs/#"))
        .await
        .expect("the wide filter subscribes");

    let subscriptions = ["jobs/+", "jobs/+", "jobs/#", "other"];
    assert_eq!(connected.routes("jobs/print", &subscriptions), [0, 2]);
    assert_eq!(connected.routes("jobs", &subscriptions), [2]);
    assert!(connected.routes("elsewhere", &subscriptions).is_empty());

    connected.shutdown().await.expect("shutdown succeeds");
}
