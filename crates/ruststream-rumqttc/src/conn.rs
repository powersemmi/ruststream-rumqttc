//! The connection task: owns the client event loop, reconnects with backoff, and
//! demultiplexes packets to subscriptions.
//!
//! The client exposes a single event loop that must be polled continuously - polling drives
//! keep-alive, acknowledgements, and flow control alike - so the task does nothing but poll:
//! subscriptions and publishes go through the cloneable `AsyncClient` from the caller's task,
//! and delivery back-pressure is the protocol's own receive-maximum (bounding unacked
//! deliveries), never a stalled loop.

use std::collections::VecDeque;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use rumqttc::v5::mqttbytes::v5::{
    ConnAck, ConnectReturnCode, Filter, Packet, RetainForwardRule, SubscribeProperties,
    SubscribeReasonCode,
};
use rumqttc::v5::mqttbytes::{QoS, matches};
use rumqttc::v5::{AsyncClient, ConnectionError, Event, EventLoop, StateError};
use tokio::sync::oneshot;

use crate::broker::Link;
use crate::error::MqttError;
use crate::message::MqttMessage;
use crate::registry::{DeliverySender, Registry, SubscribeRequest};

/// Who waits for the `SUBACK` of one subscribe.
enum Awaiting {
    /// A subscription being opened: its member and the caller waiting on the answer.
    Open {
        member: u64,
        done: oneshot::Sender<Result<(), MqttError>>,
    },
    /// A filter already on the server, re-subscribed to gain an identifier; nobody waits.
    Identify,
    /// A filter subscribed again after the server lost the session; nobody waits.
    Resubscribe,
}

/// A subscribe awaiting its `SUBACK`. The event loop emits the packet id after the request
/// leaves, in issue order, so ids are assigned first-come-first-served; every subscribe enters
/// this queue and the request channel under one guard, so the two orders are the same.
struct PendingSub {
    filter: String,
    pkid: Option<u16>,
    awaiting: Awaiting,
}

/// How many deliveries matching no subscription are held before the oldest is dropped.
///
/// A resumed session's backlog of `QoS` 1 and 2 messages is bounded by the receive-maximum this
/// crate announces (1000 by default), so this holds a full one. `QoS` 0 has no such bound, and is
/// what the eviction exists for.
const HELD_DELIVERIES: usize = 1024;

/// How long an open waits before offering its subscribe again when the client's request queue is
/// full: the queue drains only as fast as the connection task polls, which a reconnect pauses.
const REQUEST_QUEUE_RETRY: Duration = Duration::from_millis(10);

/// State shared between the connection task, the broker, and subscriber handles.
pub(crate) struct Shared {
    registry: Mutex<Registry>,
    pending: Mutex<VecDeque<PendingSub>>,
    /// Deliveries no subscription matched yet. A session resumed with `clean_start(false)`
    /// flushes what it queued immediately after `CONNACK`, before the application has opened a
    /// single subscription, so they wait here for the filter they belong to.
    /// With the subscription identifiers each named, which decide the filter that claims it.
    held: Mutex<VecDeque<(MqttMessage, Vec<usize>)>>,
    /// The last failure the connection task decided to retry, as it read on the wire. A retry
    /// leaves no other trace, so without this a startup that never reaches a `CONNACK` can only
    /// report that it waited.
    last_error: Mutex<Option<String>>,
    pub(crate) closed: AtomicBool,
    /// The connection task has stopped, so nothing drains the client's request queue any more.
    exited: AtomicBool,
    /// Whether the server's last `CONNACK` offered subscription identifiers.
    identifiers: AtomicBool,
}

impl Shared {
    pub(crate) fn new() -> Self {
        Self {
            registry: Mutex::new(Registry::default()),
            pending: Mutex::new(VecDeque::new()),
            held: Mutex::new(VecDeque::new()),
            last_error: Mutex::new(None),
            closed: AtomicBool::new(false),
            exited: AtomicBool::new(false),
            // The protocol's default when the property is absent.
            identifiers: AtomicBool::new(true),
        }
    }

    /// Holds a delivery no subscription matched, evicting the oldest when the buffer is full.
    fn hold(&self, message: MqttMessage, identifiers: &[usize]) {
        // A held delivery waits for a subscription that may never open, so the test harness stops
        // counting it as in flight: it is not a reaction the harness can wait for.
        #[cfg(feature = "testing")]
        let message = message.uncounted();
        let mut held = self.held.lock().expect("mqtt held mutex poisoned");
        if held.len() >= HELD_DELIVERIES
            && let Some((dropped, _)) = held.pop_front()
        {
            tracing::warn!(
                topic = %dropped.topic(),
                capacity = HELD_DELIVERIES,
                "mqtt delivery dropped: no subscription matches its topic and the hold buffer is full"
            );
        }
        held.push_back((message, identifiers.to_vec()));
    }

    /// Records the failure a retry is about to hide.
    fn record_error(&self, err: &ConnectionError) {
        *self.last_error.lock().expect("mqtt error mutex poisoned") = Some(err.to_string());
    }

    /// What the connection last failed with, if it has failed at all.
    pub(crate) fn last_error(&self) -> Option<String> {
        self.last_error
            .lock()
            .expect("mqtt error mutex poisoned")
            .clone()
    }

    /// How many deliveries are still waiting for a subscription to match them.
    pub(crate) fn held(&self) -> usize {
        self.held.lock().expect("mqtt held mutex poisoned").len()
    }

    pub(crate) fn ensure_open(&self) -> Result<(), MqttError> {
        if self.closed.load(Ordering::Acquire) {
            return Err(MqttError::NotConnected);
        }
        Ok(())
    }

    fn gone(&self) -> bool {
        self.closed.load(Ordering::Acquire) || self.exited.load(Ordering::Acquire)
    }

    /// Opens a local subscription on `wire_filter` and waits for the server's `SUBACK`; answers
    /// the member id the subscriber releases on drop.
    ///
    /// A filter already on the server gains a member rather than a second subscription, and a
    /// filter meeting another one on this connection is told apart from it by a subscription
    /// identifier (see [`Registry::join`]).
    // The registry guard spans the join and the backlog handover on purpose (see below).
    #[allow(clippy::significant_drop_tightening)]
    // Without the `testing` feature a link has one variant, so the match on it has a single arm;
    // it stays so that the in-process arm has its place when the feature is on.
    #[cfg_attr(
        not(feature = "testing"),
        allow(clippy::infallible_destructuring_match)
    )]
    pub(crate) async fn open(
        &self,
        link: &Link,
        wire_filter: &str,
        match_filter: &str,
        qos: QoS,
        tx: DeliverySender,
    ) -> Result<u64, MqttError> {
        self.ensure_open()?;
        let joined = {
            // The registry guard spans the handover: a live delivery needs the same guard, so
            // it cannot overtake the backlog this filter is about to receive.
            let mut registry = self.registry.lock().expect("mqtt registry mutex poisoned");
            let joined = registry.join(
                wire_filter,
                match_filter,
                qos,
                self.identifiers.load(Ordering::Relaxed),
                tx.clone(),
            )?;
            let mut held = self.held.lock().expect("mqtt held mutex poisoned");
            let mut unclaimed = VecDeque::with_capacity(held.len());
            while let Some((message, identifiers)) = held.pop_front() {
                if matches(message.topic(), match_filter)
                    && registry.claims(wire_filter, &identifiers)
                {
                    let _ = tx.send(Ok(message));
                } else {
                    unclaimed.push_back((message, identifiers));
                }
            }
            *held = unclaimed;
            joined
        };
        let client = match link {
            Link::Wire(client) => client,
            #[cfg(feature = "testing")]
            Link::InProcess(bus) => {
                // The in-process server answers a subscribe as it takes it, and always accepts.
                for request in &joined.requests {
                    if request.refresh {
                        if !self
                            .registry
                            .lock()
                            .expect("mqtt registry mutex poisoned")
                            .holds(&request.filter)
                        {
                            continue;
                        }
                        bus.subscribe(self, request);
                        self.registry
                            .lock()
                            .expect("mqtt registry mutex poisoned")
                            .identified(&request.filter, true);
                    } else {
                        bus.subscribe(self, request);
                    }
                }
                return Ok(joined.member);
            }
        };
        let (done, wait) = oneshot::channel();
        let mut done = Some(done);
        for request in &joined.requests {
            let awaiting = if request.refresh {
                Awaiting::Identify
            } else {
                Awaiting::Open {
                    member: joined.member,
                    done: done.take().expect("one request opens the member"),
                }
            };
            let mut pending = PendingSub {
                filter: request.filter.clone(),
                pkid: None,
                awaiting,
            };
            loop {
                // A refresh goes out under the registry guard, and only while its filter is still
                // held: a release that took the filter off the server first would otherwise see
                // the refresh subscribe it again, for no local subscription.
                let sent = {
                    let registry = self.registry.lock().expect("mqtt registry mutex poisoned");
                    if request.refresh && !registry.holds(&request.filter) {
                        break;
                    }
                    self.send_subscribe(client, request, pending)
                };
                match sent {
                    Ok(()) => break,
                    Err(_) if self.gone() => {
                        self.release(joined.member, link);
                        return Err(MqttError::Subscribe {
                            filter: match_filter.to_owned(),
                            reason: "the mqtt connection task has shut down".to_owned(),
                        });
                    }
                    Err(back) => {
                        pending = back;
                        tokio::time::sleep(REQUEST_QUEUE_RETRY).await;
                    }
                }
            }
        }
        wait.await.map_err(|_| MqttError::Subscribe {
            filter: match_filter.to_owned(),
            reason: "the mqtt connection task has shut down".to_owned(),
        })??;
        Ok(joined.member)
    }

    /// Queues one subscribe and its `SUBACK` record under one guard, or hands the record back
    /// when the client's request queue refuses it.
    // The pending guard spans the send on purpose: it is what keeps the two orders the same.
    #[allow(clippy::significant_drop_tightening)]
    fn send_subscribe(
        &self,
        client: &AsyncClient,
        request: &SubscribeRequest,
        record: PendingSub,
    ) -> Result<(), PendingSub> {
        let mut filter = Filter::new(request.filter.clone(), request.qos);
        if request.refresh {
            filter.retain_forward_rule = RetainForwardRule::OnNewSubscribe;
        }
        let properties = request.identifier.map(|id| SubscribeProperties {
            id: Some(id),
            user_properties: Vec::new(),
        });
        let mut pending = self.pending.lock().expect("mqtt pending mutex poisoned");
        let sent = match properties {
            Some(properties) => client.try_subscribe_many_with_properties([filter], properties),
            None => client.try_subscribe_many([filter]),
        };
        match sent {
            Ok(()) => {
                pending.push_back(record);
                Ok(())
            }
            Err(_) => Err(record),
        }
    }

    /// Takes the local subscription `member` out, and unsubscribes its filter at the server when
    /// no other local subscription shares it.
    pub(crate) fn release(&self, member: u64, link: &Link) {
        self.release_with(member, |filter| link.unsubscribe(&filter));
    }

    /// Takes the local subscription `member` out, and hands its filter to `unsubscribe` when no
    /// other local subscription shares it.
    pub(crate) fn release_with(&self, member: u64, unsubscribe: impl FnOnce(String)) {
        let mut registry = self.registry.lock().expect("mqtt registry mutex poisoned");
        // Under the guard, so an open joining the same filter right now queues its subscribe
        // after this unsubscribe and the server ends up subscribed.
        if let Some(filter) = registry.leave(member) {
            unsubscribe(filter);
        }
    }

    /// How many subscriptions to the server this connection holds for the plain filter
    /// `match_filter`: one per wire filter, whatever number of local subscriptions share it.
    #[cfg(feature = "testing")]
    pub(crate) fn wire_filters(&self, match_filter: &str) -> usize {
        self.registry
            .lock()
            .expect("mqtt registry mutex poisoned")
            .wires
            .iter()
            .filter(|wire| wire.match_filter == match_filter)
            .count()
    }

    /// Takes the local subscription `member` out after the server refused it, leaving the server
    /// as it is: a refused subscribe changed nothing there.
    fn forget(&self, member: u64) {
        self.registry
            .lock()
            .expect("mqtt registry mutex poisoned")
            .leave(member);
    }

    /// Reads what the server offers from a `CONNACK`, and subscribes again every filter a lost
    /// session took with it.
    fn connected(&self, client: &AsyncClient, connack: &ConnAck) {
        let identifiers = connack
            .properties
            .as_ref()
            .and_then(|properties| properties.subscription_identifiers_available)
            .is_none_or(|available| available != 0);
        self.identifiers.store(identifiers, Ordering::Relaxed);
        // The client never resubscribes; the broker's session-present flag is the
        // authoritative signal that our filters are gone.
        if connack.session_present {
            return;
        }
        let mut registry = self.registry.lock().expect("mqtt registry mutex poisoned");
        registry.resubscribing(identifiers);
        for wire in &registry.wires {
            if !identifiers && wire.identifier.is_some() {
                tracing::warn!(
                    filter = %wire.filter,
                    "mqtt server offers no subscription identifiers after a reconnect: a publish \
                     this filter shares with another one on the connection may be delivered to \
                     each of them more than once"
                );
            }
            let request = SubscribeRequest {
                filter: wire.filter.clone(),
                qos: wire.qos,
                identifier: wire.identifier.filter(|_| identifiers),
                refresh: false,
            };
            let record = PendingSub {
                filter: wire.filter.clone(),
                pkid: None,
                awaiting: Awaiting::Resubscribe,
            };
            if self.send_subscribe(client, &request, record).is_err() {
                tracing::warn!(filter = %wire.filter, "mqtt resubscribe failed: the request queue is full");
            }
        }
    }

    fn broadcast_error(&self, reason: &str) {
        {
            let registry = self.registry.lock().expect("mqtt registry mutex poisoned");
            for member in registry.wires.iter().flat_map(|wire| &wire.members) {
                let _ = member.tx.send(Err(MqttError::Receive(reason.to_owned())));
            }
        }
        let mut pending = self.pending.lock().expect("mqtt pending mutex poisoned");
        for sub in pending.drain(..) {
            if let Awaiting::Open { done, .. } = sub.awaiting {
                let _ = done.send(Err(MqttError::Subscribe {
                    filter: sub.filter,
                    reason: reason.to_owned(),
                }));
            }
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
        // A TLS failure is an answer about the configuration, not about the moment: an unknown
        // certificate authority, an address the server's certificate does not cover, a refused
        // client certificate all answer the same way on every attempt. Retrying one only replaces
        // its reason with a timeout.
        ConnectionError::Tls(err) => Some(format!("tls handshake failed: {err}")),
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
                // A refusal the broker issues after the TLS handshake - a client certificate it
                // wanted and did not get - reaches the client wrapped as a deserialization error,
                // which is what a corrupt stream looks like too. Reconnecting is right for one and
                // futile for the other, so the task keeps retrying and leaves the reason where
                // `connect` can name it.
                conn.shared.record_error(&err);
                tokio::time::sleep(backoff).await;
                backoff = (backoff * 2).min(Duration::from_secs(5));
            }
        }
    }
    conn.shared.exited.store(true, Ordering::Release);
}

fn handle_incoming(conn: &mut Conn, packet: Packet) {
    match packet {
        Packet::ConnAck(connack) => {
            // Recorded before `connect` wakes: a subscription opened right after it reads whether
            // the server offers identifiers.
            conn.shared.connected(&conn.client, &connack);
            if let Some(done) = conn.first_connack.take() {
                let _ = done.send(Ok(()));
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
                    .and_then(|index| pending.remove(index))
            };
            let Some(sub) = pending_sub else { return };
            let refused = match suback.return_codes.first() {
                Some(SubscribeReasonCode::Success(_)) => None,
                other => Some(format!("broker rejected the subscription: {other:?}")),
            };
            match sub.awaiting {
                Awaiting::Open { member, done } => {
                    let outcome = match refused {
                        None => Ok(()),
                        Some(reason) => {
                            conn.shared.forget(member);
                            Err(MqttError::Subscribe {
                                filter: sub.filter,
                                reason,
                            })
                        }
                    };
                    let _ = done.send(outcome);
                }
                Awaiting::Identify => {
                    conn.shared
                        .registry
                        .lock()
                        .expect("mqtt registry mutex poisoned")
                        .identified(&sub.filter, refused.is_none());
                    if let Some(reason) = refused {
                        tracing::warn!(
                            filter = %sub.filter,
                            reason = %reason,
                            "mqtt server refused a subscription identifier: a publish this filter \
                             shares with another one on the connection may reach both of them twice"
                        );
                    }
                }
                Awaiting::Resubscribe => {
                    if let Some(reason) = refused {
                        tracing::warn!(filter = %sub.filter, reason = %reason, "mqtt resubscribe refused");
                    }
                }
            }
        }
        Packet::Publish(publish) => {
            let Ok(topic) = std::str::from_utf8(&publish.topic) else {
                tracing::warn!("mqtt publish with non-utf8 topic dropped");
                return;
            };
            let identifiers = publish
                .properties
                .as_ref()
                .map_or(&[][..], |properties| &properties.subscription_identifiers);
            let client = &conn.client;
            let dead = demultiplex(&conn.shared, topic, identifiers, |acknowledges| {
                MqttMessage::new(
                    topic.to_owned(),
                    &publish,
                    acknowledges.then(|| Link::Wire(client.clone())),
                )
            });
            for member in dead {
                conn.shared
                    .release_with(member, |filter| unsubscribe(client, &filter));
            }
        }
        _ => {}
    }
}

/// Hands one PUBLISH packet the server sent this session to the subscriptions it belongs to (see
/// [`Registry::route`]), holds it when none matches, and answers the subscriptions whose stream
/// is gone, for the caller to release through its transport.
///
/// `identifiers` are the subscription identifiers the packet carries; `message` builds one
/// delivery of the packet, told whether it is the one carrying the acknowledgement. The
/// in-process mode hands its packets through here too, so both transports demultiplex alike.
pub(crate) fn demultiplex(
    shared: &Shared,
    topic: &str,
    identifiers: &[usize],
    message: impl FnMut(bool) -> MqttMessage,
) -> Vec<u64> {
    let mut dead = Vec::new();
    let mut registry = shared
        .registry
        .lock()
        .expect("mqtt registry mutex poisoned");
    if let Some(message) = registry.route(topic, identifiers, message, &mut dead) {
        // Not an error: a resumed session flushes its backlog before the application has
        // opened the subscription that owns it, so the delivery waits for that filter instead
        // of being discarded. Held under the registry guard, so an open cannot slip in between
        // the miss and the hold and leave the delivery waiting for a filter already there.
        shared.hold(message, identifiers);
    }
    drop(registry);
    dead
}

/// Takes the wire filter `filter` off the server, from a context that cannot wait for the answer.
pub(crate) fn unsubscribe(client: &AsyncClient, filter: &str) {
    if let Err(err) = client.try_unsubscribe(filter) {
        tracing::warn!(filter = %filter, error = %err, "mqtt unsubscribe failed");
    }
}
