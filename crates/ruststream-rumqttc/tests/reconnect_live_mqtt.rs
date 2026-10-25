//! A publish that returned `Ok` survives the connection it was written to, gated behind
//! `MQTT_TEST_URL`.
//!
//! The publisher reaches the broker through a relay the tests own, so a test decides when the
//! connection drops and what the broker saw before it did: the relay swallows what the client
//! writes, then ends the connection, closed without a word or with the `DISCONNECT` a restarting
//! server sends, and the client reconnects through it to a broker that never received a byte of
//! those publishes. A subscriber connected straight to the broker counts what arrived.
//!
//! `MQTT_TEST_URL=mqtt://127.0.0.1:1883 cargo test --all-features --test reconnect_live_mqtt`.

mod relay;

use std::collections::BTreeSet;
use std::pin::pin;
use std::time::Duration;

use futures::StreamExt;
use ruststream::runtime::PublishExt;
use ruststream::{
    Broker, ConnectedBroker, IncomingMessage, Outgoing, PublishPolicy, Serialized, Subscriber,
};
use ruststream_rumqttc::{ConnectedMqttBroker, MqttBroker, MqttPublish, MqttTopic, Qos};

use relay::{Relay, SERVER_SHUTTING_DOWN};

const RECV_TIMEOUT: Duration = Duration::from_secs(15);

/// How many publishes return `Ok` while the broker hears nothing of them.
const SWALLOWED: usize = 10;

fn test_url() -> Option<String> {
    match std::env::var("MQTT_TEST_URL") {
        Ok(url) if !url.is_empty() => Some(url),
        _ => {
            assert!(
                std::env::var_os("RUSTSTREAM_REQUIRE_LIVE").is_none(),
                "RUSTSTREAM_REQUIRE_LIVE is set, so the live reconnect suite must run, \
                 but MQTT_TEST_URL is missing or empty"
            );
            eprintln!("MQTT_TEST_URL is not set; skipping the live reconnect test");
            None
        }
    }
}

fn unique(name: &str) -> String {
    format!("it/{name}/{}", std::process::id())
}

/// A payload that carries its own bytes, so what the subscriber reads back is the packet.
#[derive(Outgoing, Serialized)]
struct Reading(Vec<u8>);

/// How the relay ends the connection the publishes were written to.
#[derive(Clone, Copy)]
enum End {
    /// Closed without a word.
    Cut,
    /// The `DISCONNECT` a server sends before it restarts, then closed.
    Announced,
}

/// Publishes `SWALLOWED` messages at `qos` through `relay` that the broker never receives, ends
/// the connection they were written to as `end` says, and answers the payloads a subscriber on
/// the broker received afterwards.
async fn publish_across_a_lost_connection(
    url: &str,
    relay: &Relay,
    sender: MqttBroker,
    topic: &str,
    qos: Qos,
    end: End,
) -> BTreeSet<String> {
    let watcher = MqttBroker::new(url, format!("watch-{topic}").replace('/', "-"))
        .connect()
        .await
        .expect("watcher connects");
    let mut subscriber = watcher
        .subscribe_topic(MqttTopic::new(topic).qos(qos))
        .await
        .expect("subscription opens");

    let sender: ConnectedMqttBroker = sender.connect().await.expect("sender connects");
    let publisher = MqttPublish::default()
        .qos(qos)
        .pair(&sender)
        .await
        .expect("the policy pairs with the connected broker");

    relay.swallow();
    for index in 0..SWALLOWED {
        publisher
            .message(&Reading(format!("reading-{index}").into_bytes()))
            .to(topic)
            .publish()
            .await
            .expect("the publish is handed to the client session");
    }
    match end {
        End::Cut => relay.cut().await,
        End::Announced => relay.disconnect(SERVER_SHUTTING_DOWN).await,
    }

    let mut received = BTreeSet::new();
    let mut stream = pin!(subscriber.stream());
    while received.len() < SWALLOWED {
        let Ok(next) = tokio::time::timeout(RECV_TIMEOUT, stream.next()).await else {
            break;
        };
        let message = next.expect("stream is open").expect("delivery is ok");
        received.insert(String::from_utf8_lossy(message.payload()).into_owned());
        message.ack().await.expect("ack succeeds");
    }

    sender.shutdown().await.expect("sender shuts down");
    watcher.shutdown().await.expect("watcher shuts down");
    received
}

fn every_reading() -> BTreeSet<String> {
    (0..SWALLOWED)
        .map(|index| format!("reading-{index}"))
        .collect()
}

/// A clean session is what the crate opens by default, and the broker forgets it with the
/// connection: the client sends again in the new session what the old one never had confirmed.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_clean_session_sends_again_what_the_lost_connection_never_confirmed() {
    let Some(url) = test_url() else { return };
    let relay = Relay::start(&url).await;
    let topic = unique("resend-clean");
    let sender = MqttBroker::new(
        &relay.url,
        format!("it-resend-clean-{}", std::process::id()),
    );

    let received =
        publish_across_a_lost_connection(&url, &relay, sender, &topic, Qos::AtLeastOnce, End::Cut)
            .await;

    assert_eq!(received, every_reading());
}

/// A persistent session the broker still holds is resumed, and the client retransmits into it.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_resumed_session_retransmits_what_the_lost_connection_never_confirmed() {
    let Some(url) = test_url() else { return };
    let relay = Relay::start(&url).await;
    let topic = unique("resend-resumed");
    let sender = MqttBroker::new(
        &relay.url,
        format!("it-resend-resumed-{}", std::process::id()),
    )
    .clean_start(false)
    .session_expiry(Duration::from_mins(5));

    let received =
        publish_across_a_lost_connection(&url, &relay, sender, &topic, Qos::AtLeastOnce, End::Cut)
            .await;

    assert_eq!(received, every_reading());
}

/// A server that announces its restart ends the connection, not the session: the client resumes
/// the persistent session and retransmits what it never had confirmed, at either quality of
/// service that waits for a confirmation.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_session_resumed_after_an_announced_restart_retransmits_what_was_in_flight() {
    let Some(url) = test_url() else { return };
    for (qos, name) in [(Qos::AtLeastOnce, "qos1"), (Qos::ExactlyOnce, "qos2")] {
        let relay = Relay::start(&url).await;
        let topic = unique(&format!("resend-announced-{name}"));
        let sender = MqttBroker::new(
            &relay.url,
            format!("it-resend-announced-{name}-{}", std::process::id()),
        )
        .clean_start(false)
        .session_expiry(Duration::from_mins(5));

        let received =
            publish_across_a_lost_connection(&url, &relay, sender, &topic, qos, End::Announced)
                .await;

        assert_eq!(received, every_reading(), "{name}");
    }
}
