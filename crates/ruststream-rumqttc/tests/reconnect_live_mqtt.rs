//! A publish that returned `Ok` survives the connection it was written to, gated behind
//! `MQTT_TEST_URL`.
//!
//! The publisher reaches the broker through a relay this file owns, so a test decides when the
//! connection drops and what the broker saw before it did: the relay swallows what the client
//! writes, then closes the connection, and the client reconnects through it to a broker that never
//! received a byte of those publishes. A subscriber connected straight to the broker counts what
//! arrived.
//!
//! `MQTT_TEST_URL=mqtt://127.0.0.1:1883 cargo test --all-features --test reconnect_live_mqtt`.

use std::collections::BTreeSet;
use std::pin::pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use futures::StreamExt;
use ruststream::runtime::PublishExt;
use ruststream::{
    Broker, ConnectedBroker, IncomingMessage, Outgoing, PublishPolicy, Serialized, Subscriber,
};
use ruststream_rumqttc::{ConnectedMqttBroker, MqttBroker, MqttPublish, MqttTopic, Qos};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::task::JoinHandle;

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

/// A TCP relay between the client and the broker that can stop forwarding what the client writes
/// and then drop the connection.
struct Relay {
    url: String,
    swallowing: Arc<AtomicBool>,
    pumps: Arc<Mutex<Vec<JoinHandle<()>>>>,
    accept: JoinHandle<()>,
}

impl Relay {
    async fn start(broker_url: &str) -> Self {
        let upstream = broker_url
            .split_once("://")
            .map_or(broker_url, |(_, rest)| rest)
            .trim_end_matches('/')
            .to_owned();
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("relay binds");
        let url = format!("mqtt://{}", listener.local_addr().expect("relay address"));
        let swallowing = Arc::new(AtomicBool::new(false));
        let pumps: Arc<Mutex<Vec<JoinHandle<()>>>> = Arc::default();
        let accept = tokio::spawn({
            let swallowing = Arc::clone(&swallowing);
            let pumps = Arc::clone(&pumps);
            async move {
                while let Ok((client, _)) = listener.accept().await {
                    let server = TcpStream::connect(&upstream)
                        .await
                        .expect("relay reaches the broker");
                    let (mut client_read, mut client_write) = client.into_split();
                    let (mut server_read, mut server_write) = server.into_split();
                    let swallowing = Arc::clone(&swallowing);
                    let up = tokio::spawn(async move {
                        let mut buffer = vec![0; 64 * 1024];
                        loop {
                            let read = match client_read.read(&mut buffer).await {
                                Ok(0) | Err(_) => return,
                                Ok(read) => read,
                            };
                            if swallowing.load(Ordering::Acquire) {
                                continue;
                            }
                            if server_write.write_all(&buffer[..read]).await.is_err() {
                                return;
                            }
                        }
                    });
                    let down = tokio::spawn(async move {
                        let _ = tokio::io::copy(&mut server_read, &mut client_write).await;
                    });
                    pumps.lock().expect("relay pumps").extend([up, down]);
                }
            }
        });
        Self {
            url,
            swallowing,
            pumps,
            accept,
        }
    }

    /// From now on the broker hears nothing the client writes, so nothing it sends is
    /// acknowledged either.
    fn swallow(&self) {
        self.swallowing.store(true, Ordering::Release);
    }

    /// Closes every relayed connection, and relays the next one faithfully.
    async fn cut(&self) {
        let pumps: Vec<_> = self.pumps.lock().expect("relay pumps").drain(..).collect();
        for pump in pumps {
            pump.abort();
            let _ = pump.await;
        }
        self.swallowing.store(false, Ordering::Release);
    }
}

impl Drop for Relay {
    fn drop(&mut self) {
        self.accept.abort();
        for pump in self.pumps.lock().expect("relay pumps").drain(..) {
            pump.abort();
        }
    }
}

/// Publishes `SWALLOWED` messages at `QoS` 1 through `relay` that the broker never receives,
/// drops the connection they were written to, and answers the payloads a subscriber on the broker
/// received afterwards.
async fn publish_across_a_lost_connection(
    url: &str,
    relay: &Relay,
    sender: MqttBroker,
    topic: &str,
) -> BTreeSet<String> {
    let watcher = MqttBroker::new(url, format!("watch-{topic}").replace('/', "-"))
        .connect()
        .await
        .expect("watcher connects");
    let mut subscriber = watcher
        .subscribe_topic(MqttTopic::new(topic).qos(Qos::AtLeastOnce))
        .await
        .expect("subscription opens");

    let sender: ConnectedMqttBroker = sender.connect().await.expect("sender connects");
    let publisher = MqttPublish::default()
        .qos(Qos::AtLeastOnce)
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
    relay.cut().await;

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

    let received = publish_across_a_lost_connection(&url, &relay, sender, &topic).await;

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

    let received = publish_across_a_lost_connection(&url, &relay, sender, &topic).await;

    assert_eq!(received, every_reading());
}
