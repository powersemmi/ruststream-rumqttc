//! A TCP relay between a client and the stand's broker, owned by the test that uses it, so the
//! test decides when a connection ends and how: closed without a word, or with a `DISCONNECT` the
//! server announces its restart with, which mosquitto does not send on its own.
//!
//! What the broker sends is relayed one MQTT packet at a time, so a packet the relay injects never
//! lands inside another one, and the relay can tell when the client has subscribed again.

#![allow(dead_code)]

use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::tcp::OwnedWriteHalf;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::watch;
use tokio::task::JoinHandle;

/// The `DISCONNECT` reason code a server shutting down sends.
pub(crate) const SERVER_SHUTTING_DOWN: u8 = 0x8B;

/// The `DISCONNECT` reason code a server that refuses the client's authorization sends.
pub(crate) const NOT_AUTHORIZED: u8 = 0x87;

/// The packet type of a `PUBACK`.
const PUBACK: usize = 4;

/// The packet type of a `SUBACK`.
const SUBACK: usize = 9;

/// How long a test waits for the client to come back through the relay.
const COME_BACK: Duration = Duration::from_secs(15);

/// One relayed connection: the half the relay writes to the client, and the two pumps.
struct Link {
    client: Arc<tokio::sync::Mutex<OwnedWriteHalf>>,
    pumps: [JoinHandle<()>; 2],
}

impl Link {
    /// Stops both pumps and waits until they are gone, so nothing the client wrote before reaches
    /// the broker afterwards.
    async fn close(self) {
        for pump in self.pumps {
            pump.abort();
            let _ = pump.await;
        }
    }
}

struct State {
    upstream: String,
    swallowing: AtomicBool,
    refusing: AtomicBool,
    /// How many connections the relay took to the broker.
    connections: AtomicUsize,
    links: Mutex<Vec<Link>>,
    /// How many packets of each type the broker sent since the last cut, by packet type.
    sent: watch::Sender<[usize; 16]>,
}

/// A relay to the broker at a URL, listening on a port of its own.
pub(crate) struct Relay {
    pub(crate) url: String,
    state: Arc<State>,
    accept: JoinHandle<()>,
}

impl Relay {
    pub(crate) async fn start(broker_url: &str) -> Self {
        let upstream = broker_url
            .split_once("://")
            .map_or(broker_url, |(_, rest)| rest)
            .trim_end_matches('/')
            .to_owned();
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("relay binds");
        let url = format!("mqtt://{}", listener.local_addr().expect("relay address"));
        let state = Arc::new(State {
            upstream,
            swallowing: AtomicBool::new(false),
            refusing: AtomicBool::new(false),
            connections: AtomicUsize::new(0),
            links: Mutex::new(Vec::new()),
            sent: watch::Sender::new([0; 16]),
        });
        let accept = tokio::spawn({
            let state = Arc::clone(&state);
            async move {
                while let Ok((client, _)) = listener.accept().await {
                    if state.refusing.load(Ordering::Acquire) {
                        drop(client);
                        continue;
                    }
                    state.connections.fetch_add(1, Ordering::AcqRel);
                    let link = relay(&state, client).await;
                    state.links.lock().expect("relay links").push(link);
                }
            }
        });
        Self { url, state, accept }
    }

    /// From now on the broker hears nothing the client writes, so nothing it sends is
    /// acknowledged either.
    pub(crate) fn swallow(&self) {
        self.state.swallowing.store(true, Ordering::Release);
    }

    /// From now on a client that dials the relay is hung up on before it reaches the broker.
    pub(crate) fn refuse(&self) {
        self.state.refusing.store(true, Ordering::Release);
    }

    /// How many connections the relay took to the broker so far.
    pub(crate) fn connections(&self) -> usize {
        self.state.connections.load(Ordering::Acquire)
    }

    /// Closes every relayed connection without a word, and relays the next one faithfully.
    pub(crate) async fn cut(&self) {
        let links: Vec<_> = self
            .state
            .links
            .lock()
            .expect("relay links")
            .drain(..)
            .collect();
        self.state.sent.send_replace([0; 16]);
        for link in links {
            link.close().await;
        }
        self.state.swallowing.store(false, Ordering::Release);
    }

    /// Sends the client a `DISCONNECT` with `reason`, as a server does before it goes away, then
    /// closes every relayed connection and relays the next one faithfully.
    pub(crate) async fn disconnect(&self, reason: u8) {
        let links: Vec<_> = self
            .state
            .links
            .lock()
            .expect("relay links")
            .drain(..)
            .collect();
        self.state.sent.send_replace([0; 16]);
        for link in links {
            // Fixed header, remaining length 2: the reason code and an empty property list.
            let packet = [0xE0, 0x02, reason, 0x00];
            let _ = link.client.lock().await.write_all(&packet).await;
            link.close().await;
        }
        self.state.swallowing.store(false, Ordering::Release);
    }

    /// Resolves once the broker has acknowledged a subscribe on a connection made after the last
    /// cut, which is a client that came back and subscribed again.
    ///
    /// # Panics
    ///
    /// Panics when the client does not come back within 15 seconds.
    pub(crate) async fn subscribed_again(&self) {
        self.sent(
            SUBACK,
            "the client came back through the relay and subscribed again",
        )
        .await;
    }

    /// Resolves once the broker has acknowledged a `QoS` 1 publish of the client since the last
    /// cut, so a cut after it cannot make the client send that publish again.
    ///
    /// # Panics
    ///
    /// Panics when no acknowledgement comes within 15 seconds.
    pub(crate) async fn confirmed(&self) {
        self.sent(PUBACK, "the broker acknowledged the client's publish")
            .await;
    }

    async fn sent(&self, packet_type: usize, what: &str) {
        let mut sent = self.state.sent.subscribe();
        tokio::time::timeout(COME_BACK, sent.wait_for(|sent| sent[packet_type] > 0))
            .await
            .unwrap_or_else(|_| panic!("timed out waiting until {what}"))
            .expect("the relay is alive");
    }
}

impl Drop for Relay {
    fn drop(&mut self) {
        self.accept.abort();
        for link in self.state.links.lock().expect("relay links").drain(..) {
            for pump in link.pumps {
                pump.abort();
            }
        }
    }
}

/// Connects `client` to the broker and starts the two pumps between them.
async fn relay(state: &Arc<State>, client: TcpStream) -> Link {
    let server = TcpStream::connect(&state.upstream)
        .await
        .expect("relay reaches the broker");
    let (mut client_read, client_write) = client.into_split();
    let (mut server_read, mut server_write) = server.into_split();
    let client_write = Arc::new(tokio::sync::Mutex::new(client_write));
    let up = tokio::spawn({
        let state = Arc::clone(state);
        async move {
            let mut buffer = vec![0; 64 * 1024];
            loop {
                let read = match client_read.read(&mut buffer).await {
                    Ok(0) | Err(_) => return,
                    Ok(read) => read,
                };
                if state.swallowing.load(Ordering::Acquire) {
                    continue;
                }
                if server_write.write_all(&buffer[..read]).await.is_err() {
                    return;
                }
            }
        }
    });
    let down = tokio::spawn({
        let state = Arc::clone(state);
        let client_write = Arc::clone(&client_write);
        async move {
            let mut pending = Vec::new();
            let mut buffer = vec![0; 64 * 1024];
            loop {
                let read = match server_read.read(&mut buffer).await {
                    Ok(0) | Err(_) => return,
                    Ok(read) => read,
                };
                pending.extend_from_slice(&buffer[..read]);
                while let Some(length) = packet_length(&pending) {
                    let packet: Vec<u8> = pending.drain(..length).collect();
                    if client_write.lock().await.write_all(&packet).await.is_err() {
                        return;
                    }
                    state
                        .sent
                        .send_modify(|sent| sent[usize::from(packet[0] >> 4)] += 1);
                }
            }
        }
    });
    Link {
        client: client_write,
        pumps: [up, down],
    }
}

/// The length of the whole MQTT packet at the front of `bytes`, once all of it is there.
fn packet_length(bytes: &[u8]) -> Option<usize> {
    let mut remaining = 0usize;
    for (index, byte) in bytes.iter().enumerate().skip(1).take(4) {
        remaining |= usize::from(byte & 0x7F) << (7 * (index - 1));
        if byte & 0x80 == 0 {
            let length = index + 1 + remaining;
            return (bytes.len() >= length).then_some(length);
        }
    }
    None
}
