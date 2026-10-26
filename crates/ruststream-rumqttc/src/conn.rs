//! The connection task: owns the client event loop, recovers a lost connection as the broker's
//! [`Reconnect`](crate::Reconnect) mode says, and demultiplexes packets to subscriptions.
//!
//! The client exposes a single event loop that must be polled continuously - polling drives
//! keep-alive, acknowledgements, and flow control alike - so the task does nothing but poll:
//! subscriptions and publishes go through the cloneable `AsyncClient` from the caller's task,
//! and delivery back-pressure is the protocol's own receive-maximum (bounding unacked
//! deliveries), never a stalled loop.

use std::collections::VecDeque;
use std::pin::pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use rumqttc::Outgoing;
use rumqttc::v5::mqttbytes::v5::{
    ConnAck, ConnectReturnCode, DisconnectReasonCode, Filter, Packet, RetainForwardRule,
    SubscribeProperties, SubscribeReasonCode,
};
use rumqttc::v5::mqttbytes::{QoS, matches};
use rumqttc::v5::{AsyncClient, ConnectionError, Event, EventLoop, Request, StateError};
use tokio::sync::{Notify, oneshot};

use crate::broker::Link;
use crate::error::MqttError;
use crate::message::MqttMessage;
use crate::reconnect::{Policy, Recovered, recover};
use crate::registry::{DeliverySender, Registry, SubscribeRequest};

/// Who waits for the `SUBACK` of one subscribe.
pub(crate) enum Awaiting {
    /// A subscription being opened: its member and the caller waiting on the answer.
    Open {
        member: u64,
        done: oneshot::Sender<Result<(), MqttError>>,
    },
    /// A filter already on the server, re-subscribed to gain an identifier; nobody waits.
    Identify,
    /// A filter subscribed again after the server lost the session; nobody waits.
    Resubscribe,
    /// A filter a custom reconnection restores, waiting for the answer.
    Restore {
        done: oneshot::Sender<Result<(), MqttError>>,
    },
}

/// A subscribe awaiting its `SUBACK`. The event loop emits the packet id after the request
/// leaves, in issue order, so ids are assigned first-come-first-served; every subscribe enters
/// this queue and the request channel under one guard, so the two orders are the same.
pub(crate) struct PendingSub {
    pub(crate) filter: String,
    pub(crate) pkid: Option<u16>,
    pub(crate) awaiting: Awaiting,
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
    /// Wakes a connection task recovering a lost connection when the broker shuts down.
    closing: Notify,
    /// The connection task has stopped, so nothing drains the client's request queue any more.
    exited: AtomicBool,
    /// Wakes a shutdown waiting for the connection task to flush what was queued before it.
    stopped: Notify,
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
            closing: Notify::new(),
            exited: AtomicBool::new(false),
            stopped: Notify::new(),
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

    /// Marks the broker shut down, and wakes a recovery that would otherwise keep dialing.
    pub(crate) fn close(&self) {
        self.closed.store(true, Ordering::Release);
        self.closing.notify_waiters();
    }

    /// Resolves once the broker is shut down.
    pub(crate) async fn closing(&self) {
        let notified = self.closing.notified();
        let mut notified = pin!(notified);
        // Registered before the flag is read, so a shutdown in between still wakes us.
        notified.as_mut().enable();
        if self.closed.load(Ordering::Acquire) {
            return;
        }
        notified.await;
    }

    pub(crate) fn ensure_open(&self) -> Result<(), MqttError> {
        if self.closed.load(Ordering::Acquire) {
            return Err(MqttError::NotConnected);
        }
        Ok(())
    }

    /// Resolves once the connection task has stopped.
    pub(crate) async fn stopped(&self) {
        let notified = self.stopped.notified();
        let mut notified = pin!(notified);
        // Registered before the flag is read, so a task that stops in between still wakes us.
        notified.as_mut().enable();
        if self.exited.load(Ordering::Acquire) {
            return;
        }
        notified.await;
    }

    pub(crate) fn gone(&self) -> bool {
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
                    // Counted again: the subscription's reaction to it is one the harness waits
                    // for.
                    #[cfg(feature = "testing")]
                    let message = message.recounted();
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
    pub(crate) fn send_subscribe(
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
        match link {
            Link::Wire(client) => self.release_with(member, |filter| unsubscribe(client, &filter)),
            #[cfg(feature = "testing")]
            Link::InProcess(bus) => bus.release(self, member),
        }
    }

    /// Takes the local subscription `member` out and leaves its filter on the server, where the
    /// session keeps it.
    pub(crate) fn leave(&self, member: u64) {
        self.release_with(member, |_| {});
    }

    /// Takes the local subscription `member` out, and hands its filter to `take_off` when no other
    /// local subscription shares it.
    pub(crate) fn release_with(&self, member: u64, take_off: impl FnOnce(String)) {
        let mut registry = self.registry.lock().expect("mqtt registry mutex poisoned");
        // Under the guard, so an open joining the same filter right now queues its subscribe
        // after this unsubscribe and the server ends up subscribed.
        if let Some(filter) = registry.leave(member) {
            take_off(filter);
        }
    }

    /// Which of the local subscriptions opened on the plain filter `match_filter` receive the next
    /// publish to a topic it matches, by their rank in the order they opened: one per wire filter
    /// (a share group and a plain subscription on one filter are two), the member whose turn it is.
    /// Empty when this connection holds no subscription on the filter.
    #[cfg(feature = "testing")]
    pub(crate) fn next_takers(&self, match_filter: &str) -> Vec<usize> {
        let registry = self.registry.lock().expect("mqtt registry mutex poisoned");
        let wires = || {
            registry
                .wires
                .iter()
                .filter(|wire| wire.match_filter == match_filter)
        };
        // Member ids grow in the order the subscriptions opened.
        let mut opened: Vec<u64> = wires()
            .flat_map(|wire| wire.members.iter().map(|member| member.id))
            .collect();
        opened.sort_unstable();
        wires()
            .filter_map(|wire| opened.binary_search(&wire.upcoming()).ok())
            .collect()
    }

    /// Takes the local subscription `member` out after the server refused it, leaving the server
    /// as it is: a refused subscribe changed nothing there.
    fn forget(&self, member: u64) {
        self.registry
            .lock()
            .expect("mqtt registry mutex poisoned")
            .leave(member);
    }

    /// Reads what the server offers from a `CONNACK`, and, when `restore` says so, subscribes
    /// again every filter a lost session took with it.
    fn connected(&self, client: &AsyncClient, connack: &ConnAck, restore: bool) {
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
        if !restore {
            return;
        }
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

    /// Every wire filter of this connection and the quality of service it is subscribed at, in
    /// the order they were opened.
    pub(crate) fn wires(&self) -> Vec<(String, QoS)> {
        self.registry
            .lock()
            .expect("mqtt registry mutex poisoned")
            .wires
            .iter()
            .map(|wire| (wire.filter.clone(), wire.qos))
            .collect()
    }

    /// The subscribe that puts the wire filter `filter` on the server again as it was declared,
    /// or `None` when no subscription holds it any more.
    pub(crate) fn restore_request(&self, filter: &str) -> Option<SubscribeRequest> {
        let identifiers = self.identifiers.load(Ordering::Relaxed);
        self.registry
            .lock()
            .expect("mqtt registry mutex poisoned")
            .wires
            .iter()
            .find(|wire| wire.filter == filter)
            .map(|wire| SubscribeRequest {
                filter: wire.filter.clone(),
                qos: wire.qos,
                identifier: wire.identifier.filter(|_| identifiers),
                refresh: false,
            })
    }

    /// Forgets the restores nobody waits for any more: a connection lost again took their
    /// `SUBACK`s with it, and a stale record would take the packet id of a later subscribe.
    pub(crate) fn abandon_restores(&self) {
        self.pending
            .lock()
            .expect("mqtt pending mutex poisoned")
            .retain(|sub| !matches!(&sub.awaiting, Awaiting::Restore { done } if done.is_closed()));
    }

    /// Ends the stream of every local subscription on the wire filter `filter`, after an error
    /// naming `reason`.
    pub(crate) fn end_filter(&self, filter: &str, reason: &str) {
        let mut registry = self.registry.lock().expect("mqtt registry mutex poisoned");
        let Some(index) = registry.wires.iter().position(|wire| wire.filter == filter) else {
            return;
        };
        let wire = registry.wires.swap_remove(index);
        drop(registry);
        for member in wire.members {
            let _ = member.tx.send(Err(MqttError::Receive(reason.to_owned())));
        }
    }

    /// Ends every subscription's stream after an error naming `reason`, and fails every subscribe
    /// still waiting for its answer: the connection is gone for good.
    fn end(&self, reason: &str) {
        // Closed before any stream ends, so a publish made after a stream ended fails instead of
        // landing in a request queue the task no longer drains.
        self.close();
        // Taken out of the registry, so the senders drop and every stream ends once it has
        // yielded the error.
        let wires = std::mem::take(
            &mut self
                .registry
                .lock()
                .expect("mqtt registry mutex poisoned")
                .wires,
        );
        for member in wires.into_iter().flat_map(|wire| wire.members) {
            let _ = member.tx.send(Err(MqttError::Receive(reason.to_owned())));
        }
        let mut pending = self.pending.lock().expect("mqtt pending mutex poisoned");
        for sub in pending.drain(..) {
            let refused = Err(MqttError::Subscribe {
                filter: sub.filter,
                reason: reason.to_owned(),
            });
            match sub.awaiting {
                Awaiting::Open { done, .. } | Awaiting::Restore { done } => {
                    let _ = done.send(refused);
                }
                Awaiting::Identify | Awaiting::Resubscribe => {}
            }
        }
    }
}

/// A `poll` error that means the configuration is wrong and retrying cannot help.
pub(crate) fn fatal_reason(err: &ConnectionError) -> Option<String> {
    match err {
        ConnectionError::ConnectionRefused(code) => match code {
            ConnectReturnCode::ServerUnavailable
            | ConnectReturnCode::ServerBusy
            | ConnectReturnCode::ConnectionRateExceeded
            | ConnectReturnCode::QuotaExceeded => None,
            other => Some(format!("broker refused the connection: {other:?}")),
        },
        // A server that withdraws the authorization answers a new login the same way. Every other
        // DISCONNECT, a server shutting down or a keep-alive that ran out among them, ends this
        // connection and not the next one.
        ConnectionError::MqttState(StateError::ServerDisconnect {
            reason_code: reason_code @ DisconnectReasonCode::NotAuthorized,
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
    /// What a lost connection left unconfirmed, held until the next `CONNACK` says whether the
    /// server kept the session. The client drops all of it when the session is new, and a
    /// publish in it already reported success to its caller.
    pub(crate) parked: VecDeque<Request>,
    /// What a connection lost after startup does.
    pub(crate) policy: Policy,
}

/// Whether the connection task keeps reading after an event.
#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) enum Flow {
    Continue,
    /// The `DISCONNECT` shutdown queued last is on the wire, after everything queued before it.
    Stop,
}

impl Conn {
    /// Sets aside what the connection that failed with `err` left unconfirmed, and keeps the
    /// failure where `connect` can name it.
    pub(crate) fn lost(&mut self, err: &ConnectionError) {
        // The client clears what it set aside as soon as a CONNACK reports a new session, before
        // this task sees that CONNACK.
        self.park();
        // A refusal the broker issues after the TLS handshake - a client certificate it wanted
        // and did not get - reaches the client wrapped as a deserialization error, which is what
        // a corrupt stream looks like too. Reconnecting is right for one and futile for the
        // other, so the task keeps retrying and leaves the reason where `connect` can name it.
        self.shared.record_error(err);
    }

    /// Handles one event the event loop yielded.
    pub(crate) fn handle(&mut self, event: Event) -> Flow {
        match event {
            Event::Incoming(packet) => handle_incoming(self, packet),
            Event::Outgoing(Outgoing::Subscribe(pkid)) => {
                // The loop emits packet ids in issue order; hand this one to the oldest pending
                // subscribe without one.
                let mut pending = self
                    .shared
                    .pending
                    .lock()
                    .expect("mqtt pending mutex poisoned");
                if let Some(sub) = pending.iter_mut().find(|sub| sub.pkid.is_none()) {
                    sub.pkid = Some(pkid);
                }
            }
            // The requests channel is FIFO, so every acknowledgement queued before shutdown is on
            // the wire once the DISCONNECT that shutdown queued last has been flushed.
            Event::Outgoing(Outgoing::Disconnect) => return Flow::Stop,
            Event::Outgoing(_) => {}
        }
        Flow::Continue
    }

    /// Takes over what the client set aside from the connection that just failed: the publishes
    /// it had not seen confirmed and the requests still queued.
    fn park(&mut self) {
        self.parked.append(&mut self.eventloop.pending);
    }

    /// Hands back to the client what `park` took, before anything else goes out on the new
    /// connection. A resumed session takes all of it, retransmitted the way the protocol says. A
    /// new session knows none of the old packet ids, so only the publishes go back, as fresh ones:
    /// the subscriptions are `connected`'s to restore, and the acknowledgements belonged to a
    /// session that is gone.
    fn unpark(&mut self, session_present: bool) {
        if session_present {
            self.eventloop.pending.append(&mut self.parked);
            return;
        }
        let publishes = self.parked.drain(..).filter_map(|request| match request {
            Request::Publish(mut publish) => {
                publish.pkid = 0;
                publish.dup = false;
                Some(Request::Publish(publish))
            }
            _ => None,
        });
        self.eventloop.pending.extend(publishes);
    }
}

/// Drives the event loop for the lifetime of the broker.
pub(crate) async fn run(mut conn: Conn) {
    // The client retries with zero delay forever (including on fatal auth failures), so the pause
    // before the first CONNACK is ours to own; after it, the broker's reconnect mode decides.
    let mut backoff = STARTUP_FIRST_PAUSE;
    loop {
        let err = match conn.eventloop.poll().await {
            // The delivery path, matched here so it costs what it did before the other events
            // went through `handle`.
            Ok(Event::Incoming(packet)) => {
                handle_incoming(&mut conn, packet);
                continue;
            }
            Ok(event) => {
                if conn.handle(event) == Flow::Stop {
                    break;
                }
                continue;
            }
            Err(err) => err,
        };
        if conn.shared.closed.load(Ordering::Acquire) {
            break;
        }
        if let Some(reason) = fatal_reason(&err) {
            if let Some(done) = conn.first_connack.take() {
                let _ = done.send(Err(MqttError::Connect(Box::from(reason.clone()))));
            }
            conn.shared.end(&reason);
            break;
        }
        conn.lost(&err);
        if conn.first_connack.is_some() {
            tracing::debug!(error = %err, "mqtt connection error before the first CONNACK; backing off");
            tokio::time::sleep(backoff).await;
            backoff = (backoff * 2).min(STARTUP_LONGEST_PAUSE);
            continue;
        }
        tracing::warn!(error = %err, "mqtt connection lost; recovering");
        // Boxed, so the recovery's state lives on the heap only while a connection is lost, and
        // the task polled for every delivery stays as small as it was.
        match Box::pin(recover(&mut conn, &err)).await {
            Recovered::Resumed => tracing::info!("mqtt connection recovered"),
            Recovered::GaveUp(reason) => {
                tracing::error!(reason = %reason, "mqtt connection not recovered; every subscription ends");
                conn.shared.end(&reason);
                break;
            }
            Recovered::Closing => break,
        }
    }
    conn.shared.exited.store(true, Ordering::Release);
    conn.shared.stopped.notify_waiters();
}

/// The pause after the first failed dial at startup, doubled after each later one.
const STARTUP_FIRST_PAUSE: Duration = Duration::from_millis(100);
/// The longest pause between two dials at startup.
const STARTUP_LONGEST_PAUSE: Duration = Duration::from_secs(5);

fn handle_incoming(conn: &mut Conn, packet: Packet) {
    match packet {
        Packet::ConnAck(connack) => {
            conn.unpark(connack.session_present);
            // Recorded before `connect` wakes: a subscription opened right after it reads whether
            // the server offers identifiers.
            conn.shared
                .connected(&conn.client, &connack, conn.policy.restores_on_connack());
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
                Awaiting::Restore { done } => {
                    let _ = done.send(match refused {
                        None => Ok(()),
                        Some(reason) => Err(MqttError::Subscribe {
                            filter: sub.filter,
                            reason,
                        }),
                    });
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
fn unsubscribe(client: &AsyncClient, filter: &str) {
    if let Err(err) = client.try_unsubscribe(filter) {
        tracing::warn!(filter = %filter, error = %err, "mqtt unsubscribe failed");
    }
}

#[cfg(test)]
mod tests {
    use rumqttc::v5::MqttOptions;
    use rumqttc::v5::mqttbytes::v5::{PubRel, Publish, Subscribe};

    use super::*;

    /// A connection task that has never dialed, holding what a lost connection left behind.
    fn after_a_lost_connection(left: impl IntoIterator<Item = Request>) -> Conn {
        let (client, eventloop) = AsyncClient::new(MqttOptions::new("parked", "127.0.0.1", 1), 8);
        let mut conn = Conn {
            client,
            eventloop,
            shared: Arc::new(Shared::new()),
            first_connack: None,
            parked: VecDeque::new(),
            policy: Policy::Never,
        };
        conn.eventloop.pending.extend(left);
        conn.park();
        conn
    }

    fn unconfirmed(pkid: u16) -> Request {
        let mut publish = Publish::new("readings", QoS::AtLeastOnce, "reading", None);
        publish.pkid = pkid;
        publish.dup = true;
        Request::Publish(publish)
    }

    fn left_behind() -> Vec<Request> {
        vec![
            unconfirmed(7),
            Request::PubRel(PubRel::new(3, None)),
            Request::Subscribe(Subscribe::new(
                Filter::new("readings", QoS::AtLeastOnce),
                None,
            )),
            unconfirmed(0),
        ]
    }

    fn server_disconnect(reason_code: DisconnectReasonCode) -> ConnectionError {
        ConnectionError::MqttState(StateError::ServerDisconnect {
            reason_code,
            reason_string: None,
        })
    }

    #[test]
    fn a_server_disconnect_ends_the_connection_not_the_service() {
        for code in [
            DisconnectReasonCode::ServerShuttingDown,
            DisconnectReasonCode::KeepAliveTimeout,
            DisconnectReasonCode::SessionTakenOver,
            DisconnectReasonCode::AdministrativeAction,
        ] {
            assert_eq!(fatal_reason(&server_disconnect(code)), None, "{code:?}");
        }
    }

    #[test]
    fn a_withdrawn_authorization_is_fatal() {
        let reason = fatal_reason(&server_disconnect(DisconnectReasonCode::NotAuthorized));

        assert!(
            reason.is_some_and(|reason| reason.contains("NotAuthorized")),
            "a new login would be refused the same way"
        );
    }

    #[test]
    fn a_new_session_gets_the_publishes_again_as_fresh_ones() {
        let mut conn = after_a_lost_connection(left_behind());
        assert!(
            conn.eventloop.pending.is_empty(),
            "nothing is left for the client to clear"
        );

        conn.unpark(false);

        let resent: Vec<_> = conn.eventloop.pending.drain(..).collect();
        let mut fresh = Publish::new("readings", QoS::AtLeastOnce, "reading", None);
        fresh.pkid = 0;
        fresh.dup = false;
        assert_eq!(
            resent,
            vec![Request::Publish(fresh.clone()), Request::Publish(fresh)]
        );
        assert!(conn.parked.is_empty());
    }

    #[test]
    fn a_resumed_session_gets_everything_back_as_it_was() {
        let mut conn = after_a_lost_connection(left_behind());

        conn.unpark(true);

        assert_eq!(
            conn.eventloop.pending.drain(..).collect::<Vec<_>>(),
            left_behind()
        );
        assert!(conn.parked.is_empty());
    }
}
