//! A service whose connection the server ends while it runs, gated behind `MQTT_TEST_URL`.
//!
//! The service reaches the broker through a relay the tests own, so a test decides how the
//! connection ends: with the `DISCONNECT` a server sends before it restarts, or closed without a
//! word, as mosquitto does. Each test reads a topic of its own, because the tests of this file run
//! at once against one server.
//!
//! `MQTT_TEST_URL=mqtt://127.0.0.1:1883 cargo test --all-features --test disconnect_live_mqtt`.

#![cfg(feature = "testing")]

mod relay;

use std::error::Error;
use std::num::NonZeroU32;
use std::pin::pin;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use futures::StreamExt;
use ruststream::testing::TestApp;
use ruststream::{Broker, ConnectedBroker, IncomingMessage, Subscriber};
use ruststream_rumqttc::prelude::*;
use ruststream_rumqttc::{
    ConnectedMqttBroker, MqttError, MqttSubscriber, Reconnected, Reconnector, Recovery,
};
use serde::{Deserialize, Serialize};

use relay::{NOT_AUTHORIZED, Relay, SERVER_SHUTTING_DOWN};

/// How long a stream gets to yield what the test waits for.
const RECV_TIMEOUT: Duration = Duration::from_secs(15);

fn test_url() -> Option<String> {
    match std::env::var("MQTT_TEST_URL") {
        Ok(url) if !url.is_empty() => Some(url),
        _ => {
            assert!(
                std::env::var_os("RUSTSTREAM_REQUIRE_LIVE").is_none(),
                "RUSTSTREAM_REQUIRE_LIVE is set, so the live disconnect suite must run, \
                 but MQTT_TEST_URL is missing or empty"
            );
            eprintln!("MQTT_TEST_URL is not set; skipping the live disconnect test");
            None
        }
    }
}

fn client_id(name: &str) -> String {
    format!("disconnect-{name}-{}", std::process::id())
}

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
struct Telemetry {
    device: String,
}

const ANNOUNCED: &str = "disconnect/announced";
const DROPPED: &str = "disconnect/dropped";
const CUSTOM: &str = "disconnect/custom";

#[subscriber("disconnect/announced")]
async fn record_announced(telemetry: &Telemetry) -> HandlerOutcome {
    tracing::info!(telemetry.device, "reading recorded");
    HandlerOutcome::ack()
}

#[subscriber("disconnect/dropped")]
async fn record_dropped(telemetry: &Telemetry) -> HandlerOutcome {
    tracing::info!(telemetry.device, "reading recorded");
    HandlerOutcome::ack()
}

#[subscriber("disconnect/custom")]
async fn record_custom(telemetry: &Telemetry) -> HandlerOutcome {
    tracing::info!(telemetry.device, "reading recorded");
    HandlerOutcome::ack()
}

/// The service, reading `topic`.
fn service(broker: MqttBroker, topic: &str) -> impl App<State = ()> {
    let topic = topic.to_owned();
    RustStream::new(AppInfo::new("telemetry", "0.1.0")).with_broker(broker, move |b| {
        match topic.as_str() {
            ANNOUNCED => {
                b.include(record_announced);
            }
            DROPPED => {
                b.include(record_dropped);
            }
            _ => {
                b.include(record_custom);
            }
        }
    })
}

fn reading(device: &str) -> Telemetry {
    Telemetry {
        device: device.to_owned(),
    }
}

/// Publishes one reading to `topic` and asserts the service handled it, `calls` readings in all.
async fn reads(
    tb: &TestApp<()>,
    topic: &str,
    device: &str,
    calls: usize,
) -> Result<(), Box<dyn Error>> {
    tb.broker::<MqttBroker>()
        .message(&reading(device))
        .to(topic)
        .publish()
        .await?;
    tb.broker::<MqttBroker>()
        .subscriber(topic)
        .assert_called(calls)
        .with(&reading(device))
        .settled(HandlerOutcome::ack());
    Ok(())
}

/// The server announces its restart with a `DISCONNECT`: the service dials again, subscribes
/// again in the new session, and keeps reading.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_service_keeps_reading_after_the_server_announces_a_restart() -> Result<(), Box<dyn Error>>
{
    let Some(url) = test_url() else { return Ok(()) };
    let relay = Relay::start(&url).await;
    let broker = MqttBroker::new(&relay.url, client_id("announced"));
    let tb = TestApp::start_live(service(broker, ANNOUNCED)).await?;
    reads(&tb, ANNOUNCED, "before", 1).await?;
    // Confirmed, so the client has nothing of it to send again on the next connection.
    relay.confirmed().await;

    relay.disconnect(SERVER_SHUTTING_DOWN).await;
    relay.subscribed_again().await;

    reads(&tb, ANNOUNCED, "after", 2).await?;
    tb.shutdown().await?;
    Ok(())
}

/// The server goes away without a word, as mosquitto does: the service dials again, subscribes
/// again in the new session, and keeps reading.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_service_keeps_reading_after_the_connection_drops_without_a_word()
-> Result<(), Box<dyn Error>> {
    let Some(url) = test_url() else { return Ok(()) };
    let relay = Relay::start(&url).await;
    let broker = MqttBroker::new(&relay.url, client_id("dropped"));
    let tb = TestApp::start_live(service(broker, DROPPED)).await?;
    reads(&tb, DROPPED, "before", 1).await?;
    relay.confirmed().await;

    relay.cut().await;
    relay.subscribed_again().await;

    reads(&tb, DROPPED, "after", 2).await?;
    tb.shutdown().await?;
    Ok(())
}

/// A reconnection that dials until the server answers, then restores every subscription, and
/// notes why each connection it recovered ended.
struct RestoreEverything {
    reasons: Arc<Mutex<Vec<Option<u8>>>>,
}

impl Reconnector for RestoreEverything {
    async fn reconnect(&self, recovery: &mut Recovery<'_>) -> Result<Reconnected, MqttError> {
        self.reasons
            .lock()
            .expect("reasons")
            .push(recovery.lost().server_reason_code());
        let reconnected = loop {
            match recovery.connect().await {
                Ok(reconnected) => break reconnected,
                Err(_) => tokio::time::sleep(Duration::from_millis(50)).await,
            }
        };
        if !reconnected.session_present() {
            recovery.restore_all().await?;
        }
        Ok(reconnected)
    }
}

/// The service's own reconnection runs in place of the client's, and what it restores reads on.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_custom_reconnection_recovers_the_service() -> Result<(), Box<dyn Error>> {
    let Some(url) = test_url() else { return Ok(()) };
    let relay = Relay::start(&url).await;
    let reasons = Arc::default();
    let broker = MqttBroker::new(&relay.url, client_id("custom")).reconnect(Reconnect::custom(
        RestoreEverything {
            reasons: Arc::clone(&reasons),
        },
    ));
    let tb = TestApp::start_live(service(broker, CUSTOM)).await?;
    reads(&tb, CUSTOM, "before", 1).await?;
    relay.confirmed().await;

    relay.disconnect(SERVER_SHUTTING_DOWN).await;
    relay.subscribed_again().await;

    reads(&tb, CUSTOM, "after", 2).await?;
    assert_eq!(
        *reasons.lock().expect("reasons"),
        vec![Some(SERVER_SHUTTING_DOWN)],
        "one recovery, of a connection the server ended shutting down"
    );
    tb.shutdown().await?;
    Ok(())
}

/// A broker of the test's own on `url`, and a subscription to `topic` on it.
async fn subscribed(
    broker: MqttBroker,
    topic: &str,
) -> Result<(ConnectedMqttBroker, MqttSubscriber), MqttError> {
    let connected = broker.connect().await?;
    let subscriber = connected
        .subscribe_topic(MqttTopic::new(topic).qos(Qos::AtLeastOnce))
        .await?;
    Ok((connected, subscriber))
}

/// Reads `subscriber` until its stream ends, and answers the errors it yielded on the way.
async fn errors_until_the_end(subscriber: &mut MqttSubscriber) -> Vec<String> {
    let mut stream = pin!(subscriber.stream());
    let mut errors = Vec::new();
    loop {
        let next = tokio::time::timeout(RECV_TIMEOUT, stream.next())
            .await
            .expect("the stream ends");
        match next {
            None => return errors,
            Some(Err(err)) => errors.push(err.to_string()),
            Some(Ok(message)) => panic!("unexpected delivery on {}", message.topic()),
        }
    }
}

fn unique(name: &str) -> String {
    format!("it/disconnect/{name}/{}", std::process::id())
}

/// Without reconnection a lost connection ends the stream after an error naming why, and a
/// publish fails from then on.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn without_reconnection_a_lost_connection_ends_the_stream() -> Result<(), Box<dyn Error>> {
    let Some(url) = test_url() else { return Ok(()) };
    let relay = Relay::start(&url).await;
    let topic = unique("never");
    let (connected, mut subscriber) = subscribed(
        MqttBroker::new(&relay.url, client_id("never")).reconnect(Reconnect::never()),
        &topic,
    )
    .await?;

    relay.disconnect(SERVER_SHUTTING_DOWN).await;

    let errors = errors_until_the_end(&mut subscriber).await;
    assert_eq!(errors.len(), 1, "{errors:?}");
    assert!(
        errors[0].contains("does not reconnect") && errors[0].contains("ServerShuttingDown"),
        "{errors:?}"
    );
    let publish = connected
        .publisher()
        .message(&reading("late"))
        .to(&topic)
        .publish()
        .await;
    assert!(publish.is_err(), "a publish after the end fails");
    connected.shutdown().await?;
    Ok(())
}

/// The client's reconnection gives up after the dials it was given, and the stream ends.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_automatic_reconnection_gives_up_after_its_attempts() -> Result<(), Box<dyn Error>> {
    let Some(url) = test_url() else { return Ok(()) };
    let relay = Relay::start(&url).await;
    let attempts = NonZeroU32::new(3).expect("non-zero");
    let reconnect = Reconnect::automatic(
        AutoReconnect::new()
            .pauses(Duration::from_millis(10), Duration::from_millis(40))
            .attempts(attempts),
    );
    let (connected, mut subscriber) = subscribed(
        MqttBroker::new(&relay.url, client_id("attempts")).reconnect(reconnect),
        &unique("attempts"),
    )
    .await?;

    relay.refuse();
    relay.cut().await;

    let errors = errors_until_the_end(&mut subscriber).await;
    assert_eq!(errors.len(), 1, "{errors:?}");
    assert!(errors[0].contains("3 dials failed"), "{errors:?}");
    connected.shutdown().await?;
    Ok(())
}

/// A server that withdraws the authorization refuses the next login too, so the client does not
/// dial again: the stream ends whatever the mode.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_withdrawn_authorization_ends_the_stream_without_a_dial() -> Result<(), Box<dyn Error>> {
    let Some(url) = test_url() else { return Ok(()) };
    let relay = Relay::start(&url).await;
    let (connected, mut subscriber) = subscribed(
        MqttBroker::new(&relay.url, client_id("unauthorized")),
        &unique("unauthorized"),
    )
    .await?;

    relay.disconnect(NOT_AUTHORIZED).await;

    let errors = errors_until_the_end(&mut subscriber).await;
    assert_eq!(errors.len(), 1, "{errors:?}");
    assert!(errors[0].contains("NotAuthorized"), "{errors:?}");
    assert_eq!(relay.connections(), 1, "the client did not dial again");
    connected.shutdown().await?;
    Ok(())
}

/// Ends every subscription on the filters it is told to drop and restores the rest.
struct RestoreSome {
    dropped: String,
}

impl Reconnector for RestoreSome {
    async fn reconnect(&self, recovery: &mut Recovery<'_>) -> Result<Reconnected, MqttError> {
        let reconnected = recovery.connect().await?;
        for subscription in recovery.subscriptions() {
            if subscription.filter() == self.dropped {
                recovery.end(&subscription);
            } else {
                recovery.restore(&subscription).await?;
            }
        }
        Ok(reconnected)
    }
}

/// A custom reconnection decides what to restore: the subscription it restored reads on, and the
/// one it ended stops.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_custom_reconnection_restores_what_it_chooses() -> Result<(), Box<dyn Error>> {
    let Some(url) = test_url() else { return Ok(()) };
    let relay = Relay::start(&url).await;
    let kept_topic = unique("kept");
    let dropped_topic = unique("dropped");
    let reconnect = Reconnect::custom(RestoreSome {
        dropped: dropped_topic.clone(),
    });
    let (connected, mut kept) = subscribed(
        MqttBroker::new(&relay.url, client_id("chooses")).reconnect(reconnect),
        &kept_topic,
    )
    .await?;
    let mut dropped = connected
        .subscribe_topic(MqttTopic::new(&dropped_topic).qos(Qos::AtLeastOnce))
        .await?;

    relay.disconnect(SERVER_SHUTTING_DOWN).await;
    relay.subscribed_again().await;

    let errors = errors_until_the_end(&mut dropped).await;
    assert_eq!(errors.len(), 1, "{errors:?}");
    assert!(errors[0].contains("did not restore"), "{errors:?}");

    let sender = MqttBroker::new(&url, client_id("chooses-sender"))
        .connect()
        .await?;
    sender
        .publisher()
        .message(&reading("after"))
        .to(&kept_topic)
        .publish()
        .await?;
    let mut stream = pin!(kept.stream());
    let message = tokio::time::timeout(RECV_TIMEOUT, stream.next())
        .await?
        .expect("the restored stream is open")?;
    assert_eq!(message.topic(), kept_topic);
    message.ack().await?;

    sender.shutdown().await?;
    connected.shutdown().await?;
    Ok(())
}

/// A shutdown while the client dials a server that is gone returns at once, instead of waiting
/// for a dial that cannot succeed.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_shutdown_during_a_reconnection_returns_at_once() -> Result<(), Box<dyn Error>> {
    let Some(url) = test_url() else { return Ok(()) };
    let relay = Relay::start(&url).await;
    let reconnect = Reconnect::automatic(
        AutoReconnect::new().pauses(Duration::from_secs(3600), Duration::from_secs(3600)),
    );
    let (connected, _subscriber) = subscribed(
        MqttBroker::new(&relay.url, client_id("closing")).reconnect(reconnect),
        &unique("closing"),
    )
    .await?;

    relay.cut().await;
    tokio::time::timeout(Duration::from_secs(2), connected.shutdown())
        .await
        .map_err(|_| "the shutdown waited for the reconnection")??;
    Ok(())
}
