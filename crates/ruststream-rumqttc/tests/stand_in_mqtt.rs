//! The in-process transport answering the descriptor the way a server does.
//!
//! Each test here is the twin of one in `integration_mqtt.rs`, written in the same shape against
//! `MqttTestBroker` instead of Mosquitto: a service reading a stand-in assertion must be able to
//! find the live one that backs it. Where the two answers differ - a `QoS` handshake, a retained
//! message, a session that redelivers - there is no twin here, and the live file is the only
//! place the behaviour is claimed at all.

#![cfg(feature = "testing")]

use std::pin::pin;
use std::time::Duration;

use futures::StreamExt;
use ruststream::{
    AckError, Broker, ConnectedBroker, IncomingMessage, OutgoingMessage, Publisher, Subscriber,
};
use ruststream_rumqttc::testing::{ConnectedMqttTestBroker, MqttTestBroker};
use ruststream_rumqttc::{MqttPublishOptions, MqttTopic, Qos};

/// Long enough that a delivery which is coming has arrived; the transport is a channel, so
/// nothing here waits on a network.
const SETTLE: Duration = Duration::from_millis(100);

/// The one per-message argument these tests take: publish at `qos`, whatever the policy declares.
fn at(qos: Qos) -> MqttPublishOptions {
    MqttPublishOptions::default().qos(qos)
}

async fn connected() -> ConnectedMqttTestBroker {
    MqttTestBroker::new()
        .connect()
        .await
        .expect("the in-process broker connects")
}

/// The twin of `shared_subscriptions_split_the_stream`. A group exists to make consumers compete,
/// so the stand-in hands each message to one member: a copy each would let a test claim work was
/// shared while both members did it.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn shared_subscriptions_split_the_stream_in_process() {
    let connected = connected().await;

    let mut first = connected
        .subscribe_topic(MqttTopic::new("jobs").shared("workers"))
        .await
        .expect("first consumer subscribes");
    let mut second = connected
        .subscribe_topic(MqttTopic::new("jobs").shared("workers"))
        .await
        .expect("second consumer subscribes");

    let publisher = connected.publisher();
    for i in 0..4u8 {
        publisher
            .publish(OutgoingMessage::new("jobs", [i].as_slice()), None)
            .await
            .expect("publish succeeds");
    }

    let mut s1 = pin!(first.stream());
    let mut s2 = pin!(second.stream());
    let mut seen = 0;
    while seen < 4 {
        let message = tokio::time::timeout(SETTLE, async {
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

    let extra = tokio::time::timeout(SETTLE, async {
        tokio::select! {
            m = s1.next() => m,
            m = s2.next() => m,
        }
    })
    .await;
    assert!(
        extra.is_err(),
        "four publishes are four deliveries across the group, not one per member"
    );

    connected.shutdown().await.expect("shutdown succeeds");
}

/// Two groups are two subscriptions as far as a server is concerned, so each takes its own copy
/// and competes only within itself.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn separate_groups_each_take_their_own_copy_in_process() {
    let connected = connected().await;

    let mut workers = connected
        .subscribe_topic(MqttTopic::new("jobs").shared("workers"))
        .await
        .expect("the worker subscribes");
    let mut auditors = connected
        .subscribe_topic(MqttTopic::new("jobs").shared("auditors"))
        .await
        .expect("the auditor subscribes");

    connected
        .publisher()
        .publish(OutgoingMessage::new("jobs", b"one".as_slice()), None)
        .await
        .expect("publish succeeds");

    for stream in [&mut workers, &mut auditors] {
        let mut stream = pin!(stream.stream());
        let message = tokio::time::timeout(SETTLE, stream.next())
            .await
            .expect("each group receives the message")
            .expect("stream is open")
            .expect("delivery is ok");
        assert_eq!(message.payload(), b"one");
        message.ack().await.expect("ack succeeds");
    }

    connected.shutdown().await.expect("shutdown succeeds");
}

/// The twin of `qos0_reports_ack_unsupported`. A `QoS` 0 delivery carries no acknowledgement on
/// the wire, so it carries none here either: a stand-in that settled it would let a test prove a
/// guarantee the transport never offered.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn qos0_reports_ack_unsupported_in_process() {
    let connected = connected().await;

    let mut subscriber = connected
        .subscribe_topic(MqttTopic::new("fire").qos(Qos::AtMostOnce))
        .await
        .expect("subscription opens");

    connected
        .publisher()
        .publish(OutgoingMessage::new("fire", b"fire".as_slice()), None)
        .await
        .expect("publish succeeds");

    let mut stream = pin!(subscriber.stream());
    let message = tokio::time::timeout(SETTLE, stream.next())
        .await
        .expect("delivery arrives")
        .expect("stream is open")
        .expect("delivery is ok");
    assert!(matches!(message.ack().await, Err(AckError::Unsupported)));

    connected.shutdown().await.expect("shutdown succeeds");
}

/// A delivery comes out at the lesser of the two levels, so publishing fire-and-forget into an
/// acknowledged subscription is still a delivery that cannot be settled. Reading the subscription
/// alone would let a test hold a guarantee the publisher never asked for.
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

    let mut stream = pin!(subscriber.stream());
    let message = tokio::time::timeout(SETTLE, stream.next())
        .await
        .expect("delivery arrives")
        .expect("stream is open")
        .expect("delivery is ok");
    assert!(matches!(message.ack().await, Err(AckError::Unsupported)));

    connected.shutdown().await.expect("shutdown succeeds");
}

/// An acknowledged subscription settles, which is what makes the `QoS` 0 answer above a decision
/// rather than an inability.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_acknowledged_subscription_settles_in_process() {
    let connected = connected().await;

    let mut subscriber = connected
        .subscribe_topic(MqttTopic::new("orders").qos(Qos::AtLeastOnce))
        .await
        .expect("subscription opens");

    connected
        .publisher()
        .publish(OutgoingMessage::new("orders", b"one".as_slice()), None)
        .await
        .expect("publish succeeds");

    let mut stream = pin!(subscriber.stream());
    let message = tokio::time::timeout(SETTLE, stream.next())
        .await
        .expect("delivery arrives")
        .expect("stream is open")
        .expect("delivery is ok");
    message
        .ack()
        .await
        .expect("an acknowledged delivery settles");

    connected.shutdown().await.expect("shutdown succeeds");
}

/// A subscription cannot be opened on a connection that is gone. The owner cannot try - the
/// ladder consumed the connected form - but a clone handed out earlier can, and it is refused
/// rather than registered against a dead broker.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_aliased_handle_cannot_subscribe_after_shutdown() {
    let connected = connected().await;
    let alias = connected.clone();

    connected.shutdown().await.expect("shutdown succeeds");

    alias
        .subscribe_topic(MqttTopic::new("orders"))
        .await
        .expect_err("a subscription after shutdown must be refused");
}

/// The twin of `nack_reports_unsupported_and_dropping_acknowledges`, for the half of it this
/// transport already answers the same way: declining redelivery acknowledges. The other half -
/// `nack(requeue = true)` reporting `Unsupported` - is the one answer here that is still the
/// framework's rather than the wire's, and it is stated on `MqttTestMessage`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn dropping_acknowledges_in_process() {
    let connected = connected().await;

    let mut subscriber = connected
        .subscribe_topic(MqttTopic::new("orders").qos(Qos::AtLeastOnce))
        .await
        .expect("subscription opens");

    connected
        .publisher()
        .publish(OutgoingMessage::new("orders", b"one".as_slice()), None)
        .await
        .expect("publish succeeds");

    let mut stream = pin!(subscriber.stream());
    let message = tokio::time::timeout(SETTLE, stream.next())
        .await
        .expect("delivery arrives")
        .expect("stream is open")
        .expect("delivery is ok");
    message
        .nack(false)
        .await
        .expect("declining redelivery acknowledges");

    let redelivered = tokio::time::timeout(SETTLE, stream.next()).await;
    assert!(
        redelivered.is_err(),
        "a dropped delivery is terminal, as an acknowledgement is"
    );

    connected.shutdown().await.expect("shutdown succeeds");
}
