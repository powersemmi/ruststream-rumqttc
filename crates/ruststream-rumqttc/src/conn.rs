//! The connection task: owns the client event loop, reconnects with backoff, and
//! demultiplexes packets to subscriptions.
//!
//! The client exposes a single event loop that must be polled continuously - polling drives
//! keep-alive, acknowledgements, and flow control alike - so the task does nothing but poll:
//! subscriptions and publishes go through the cloneable `AsyncClient` from the caller's task,
//! and delivery back-pressure is the protocol's own receive-maximum (bounding unacked
//! deliveries), never a stalled loop.

use std::collections::VecDeque;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use rumqttc::v5::mqttbytes::v5::{ConnectReturnCode, Packet, SubscribeReasonCode};
use rumqttc::v5::mqttbytes::{QoS, matches};
use rumqttc::v5::{AsyncClient, ConnectionError, Event, EventLoop, StateError};
use tokio::sync::{mpsc, oneshot};

use crate::error::MqttError;
use crate::message::MqttMessage;

/// One live subscription: the wire filter (possibly `$share/...`), the stripped filter used
/// for matching, and the channel deliveries flow through.
pub(crate) struct SubEntry {
    pub(crate) id: u64,
    pub(crate) wire_filter: String,
    pub(crate) match_filter: String,
    pub(crate) qos: QoS,
    pub(crate) tx: mpsc::UnboundedSender<Result<MqttMessage, MqttError>>,
}

/// A subscribe awaiting its `SUBACK`. The event loop emits the packet id after the request
/// leaves, in issue order, so ids are assigned first-come-first-served.
struct PendingSub {
    entry_id: u64,
    filter: String,
    pkid: Option<u16>,
    done: oneshot::Sender<Result<(), MqttError>>,
}

/// State shared between the connection task, the broker, and subscriber handles.
pub(crate) struct Shared {
    pub(crate) subs: Mutex<Vec<SubEntry>>,
    pending: Mutex<VecDeque<PendingSub>>,
    pub(crate) closed: AtomicBool,
    next_id: AtomicU64,
    /// Rotates local delivery across entries sharing one wire filter (a shared group
    /// subscribed twice on this client is still one broker subscription).
    round_robin: AtomicU64,
}

impl Shared {
    pub(crate) fn new() -> Self {
        Self {
            subs: Mutex::new(Vec::new()),
            pending: Mutex::new(VecDeque::new()),
            closed: AtomicBool::new(false),
            next_id: AtomicU64::new(0),
            round_robin: AtomicU64::new(0),
        }
    }

    pub(crate) fn ensure_open(&self) -> Result<(), MqttError> {
        if self.closed.load(Ordering::Acquire) {
            return Err(MqttError::NotConnected);
        }
        Ok(())
    }

    /// Registers a subscription and its pending `SUBACK` resolver; returns the entry id.
    pub(crate) fn register(
        &self,
        wire_filter: String,
        match_filter: String,
        qos: QoS,
        tx: mpsc::UnboundedSender<Result<MqttMessage, MqttError>>,
        done: oneshot::Sender<Result<(), MqttError>>,
    ) -> u64 {
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        self.subs
            .lock()
            .expect("mqtt registry mutex poisoned")
            .push(SubEntry {
                id,
                wire_filter: wire_filter.clone(),
                match_filter,
                qos,
                tx,
            });
        self.pending
            .lock()
            .expect("mqtt pending mutex poisoned")
            .push_back(PendingSub {
                entry_id: id,
                filter: wire_filter,
                pkid: None,
                done,
            });
        id
    }

    pub(crate) fn remove(&self, id: u64) -> Option<String> {
        let mut subs = self.subs.lock().expect("mqtt registry mutex poisoned");
        subs.iter()
            .position(|entry| entry.id == id)
            .map(|index| subs.swap_remove(index).wire_filter)
    }

    fn broadcast_error(&self, reason: &str) {
        {
            let subs = self.subs.lock().expect("mqtt registry mutex poisoned");
            for entry in subs.iter() {
                let _ = entry.tx.send(Err(MqttError::Receive(reason.to_owned())));
            }
        }
        let mut pending = self.pending.lock().expect("mqtt pending mutex poisoned");
        for sub in pending.drain(..) {
            let _ = sub.done.send(Err(MqttError::Subscribe {
                filter: sub.filter,
                reason: reason.to_owned(),
            }));
        }
    }
}

/// A `poll` error that means the configuration is wrong and retrying cannot help.
fn fatal_reason(err: &ConnectionError) -> Option<String> {
    match err {
        ConnectionError::ConnectionRefused(code) => match code {
            ConnectReturnCode::ServerUnavailable
            | ConnectReturnCode::ServerBusy
            | ConnectReturnCode::ConnectionRateExceeded
            | ConnectReturnCode::QuotaExceeded => None,
            other => Some(format!("broker refused the connection: {other:?}")),
        },
        ConnectionError::MqttState(StateError::ServerDisconnect {
            reason_code,
            reason_string,
        }) => Some(format!(
            "broker disconnected the session: {reason_code:?} {reason_string:?}"
        )),
        ConnectionError::NotConnAck(_) => Some("the peer is not an MQTT broker".to_owned()),
        _ => None,
    }
}

pub(crate) struct Conn {
    pub(crate) client: AsyncClient,
    pub(crate) eventloop: EventLoop,
    pub(crate) shared: Arc<Shared>,
    pub(crate) first_connack: Option<oneshot::Sender<Result<(), MqttError>>>,
}

/// Drives the event loop for the lifetime of the broker.
pub(crate) async fn run(mut conn: Conn) {
    // The client retries with zero delay forever (including on fatal auth failures), so the
    // backoff is ours to own.
    let mut backoff = Duration::from_millis(100);
    loop {
        if conn.shared.closed.load(Ordering::Acquire) {
            break;
        }
        match conn.eventloop.poll().await {
            Ok(Event::Incoming(packet)) => {
                backoff = Duration::from_millis(100);
                handle_incoming(&mut conn, packet);
            }
            Ok(Event::Outgoing(rumqttc::Outgoing::Subscribe(pkid))) => {
                // The loop emits packet ids in issue order; hand this one to the oldest
                // pending subscribe without one.
                let mut pending = conn
                    .shared
                    .pending
                    .lock()
                    .expect("mqtt pending mutex poisoned");
                if let Some(sub) = pending.iter_mut().find(|sub| sub.pkid.is_none()) {
                    sub.pkid = Some(pkid);
                }
            }
            Ok(Event::Outgoing(_)) => {}
            Err(err) => {
                if conn.shared.closed.load(Ordering::Acquire) {
                    break;
                }
                if let Some(reason) = fatal_reason(&err) {
                    if let Some(done) = conn.first_connack.take() {
                        let _ = done.send(Err(MqttError::Connect(Box::from(reason.clone()))));
                    }
                    conn.shared.broadcast_error(&reason);
                    break;
                }
                tracing::debug!(error = %err, "mqtt connection error; backing off");
                tokio::time::sleep(backoff).await;
                backoff = (backoff * 2).min(Duration::from_secs(5));
            }
        }
    }
}

// The registry guard intentionally spans grouping and delivery: entries must not move under
// the borrowed group table.
#[allow(clippy::significant_drop_tightening)]
fn handle_incoming(conn: &mut Conn, packet: Packet) {
    match packet {
        Packet::ConnAck(connack) => {
            if let Some(done) = conn.first_connack.take() {
                let _ = done.send(Ok(()));
            }
            // The client never resubscribes; the broker's session-present flag is the
            // authoritative signal that our filters are gone.
            if !connack.session_present {
                let subs = conn
                    .shared
                    .subs
                    .lock()
                    .expect("mqtt registry mutex poisoned");
                for entry in subs.iter() {
                    if let Err(err) = conn
                        .client
                        .try_subscribe(entry.wire_filter.clone(), entry.qos)
                    {
                        tracing::warn!(filter = %entry.wire_filter, error = %err, "mqtt resubscribe failed");
                    }
                }
            }
        }
        Packet::SubAck(suback) => {
            let pending_sub = {
                let mut pending = conn
                    .shared
                    .pending
                    .lock()
                    .expect("mqtt pending mutex poisoned");
                pending
                    .iter()
                    .position(|sub| sub.pkid == Some(suback.pkid))
                    .map(|index| pending.remove(index).expect("index just found"))
            };
            if let Some(sub) = pending_sub {
                let outcome = match suback.return_codes.first() {
                    Some(SubscribeReasonCode::Success(_)) => Ok(()),
                    other => {
                        conn.shared.remove(sub.entry_id);
                        Err(MqttError::Subscribe {
                            filter: sub.filter,
                            reason: format!("broker rejected the subscription: {other:?}"),
                        })
                    }
                };
                let _ = sub.done.send(outcome);
            }
        }
        Packet::Publish(publish) => {
            let Ok(topic) = std::str::from_utf8(&publish.topic) else {
                tracing::warn!("mqtt publish with non-utf8 topic dropped");
                return;
            };
            let topic = topic.to_owned();
            let mut dead = Vec::new();
            {
                let subs = conn
                    .shared
                    .subs
                    .lock()
                    .expect("mqtt registry mutex poisoned");
                // Entries sharing one wire filter are one broker subscription (a shared
                // group subscribed twice on this client), so each such group receives one
                // copy, rotated across its entries. The wire acknowledgement belongs to
                // exactly one delivery; the first group carries it, genuinely different
                // overlapping filters get settled copies.
                let mut groups: Vec<(&str, Vec<&SubEntry>)> = Vec::new();
                for entry in subs.iter() {
                    if matches(&topic, &entry.match_filter) {
                        match groups
                            .iter_mut()
                            .find(|(wire, _)| *wire == entry.wire_filter)
                        {
                            Some((_, entries)) => entries.push(entry),
                            None => groups.push((&entry.wire_filter, vec![entry])),
                        }
                    }
                }
                let rotation =
                    usize::try_from(conn.shared.round_robin.fetch_add(1, Ordering::Relaxed))
                        .unwrap_or(0);
                let mut acker = Some(conn.client.clone());
                for (_, entries) in &groups {
                    let entry = entries[rotation % entries.len()];
                    let message = MqttMessage::new(topic.clone(), &publish, acker.take());
                    if entry.tx.send(Ok(message)).is_err() {
                        dead.push(entry.id);
                    }
                }
            }
            for id in dead {
                if let Some(filter) = conn.shared.remove(id) {
                    let _ = conn.client.try_unsubscribe(filter);
                }
            }
        }
        _ => {}
    }
}
