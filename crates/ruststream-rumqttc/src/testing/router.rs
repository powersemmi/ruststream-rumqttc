//! Subscription registry and fanout for the in-process MQTT stand-in.
//!
//! Core routing only: an exact-name match fans a published message out to every live
//! subscription on that name, and a per-name log records traffic for assertions. MQTT's own
//! semantics (wildcard filters, `QoS` handshakes, retained messages, shared subscriptions) are
//! transport behaviour and are not simulated here.

use std::collections::HashMap;
use std::sync::{
    Mutex,
    atomic::{AtomicU64, Ordering},
};

use bytes::Bytes;
use ruststream::{HeaderMap, RawMessage, testing::Coordinator};
use tokio::sync::mpsc;

/// Opaque handle identifying one subscription inside an [`AddressRouter`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) struct SubscriptionId(u64);

/// Single delivery handed to a matching subscriber.
#[derive(Debug, Clone)]
pub(crate) struct Delivery {
    pub(crate) payload: Bytes,
    pub(crate) headers: HeaderMap,
}

pub(crate) type DeliverySender = mpsc::UnboundedSender<Delivery>;
pub(crate) type DeliveryReceiver = mpsc::UnboundedReceiver<Delivery>;

struct Subscription {
    address: String,
    sender: DeliverySender,
}

#[derive(Default)]
struct RouterState {
    subscriptions: HashMap<SubscriptionId, Subscription>,
    log: HashMap<String, Vec<RawMessage>>,
}

/// In-memory exact-address router.
#[derive(Default)]
pub(crate) struct AddressRouter {
    state: Mutex<RouterState>,
    next_id: AtomicU64,
}

impl AddressRouter {
    /// Registers a subscription on `address` and returns the channel pair the subscriber will
    /// use, together with the [`SubscriptionId`] needed to unsubscribe.
    ///
    /// The returned [`DeliverySender`] is the same one fanout uses, so subscribers can re-send
    /// a delivery into their own queue to implement `nack(requeue = true)`.
    pub(crate) fn subscribe(
        &self,
        address: String,
    ) -> (SubscriptionId, DeliverySender, DeliveryReceiver) {
        let (tx, rx) = mpsc::unbounded_channel();
        let id = SubscriptionId(self.next_id.fetch_add(1, Ordering::Relaxed));
        self.state
            .lock()
            .expect("mqtt test router mutex poisoned")
            .subscriptions
            .insert(
                id,
                Subscription {
                    address,
                    sender: tx.clone(),
                },
            );
        (id, tx, rx)
    }

    /// Removes a subscription. No-op if the id is unknown (double-drop of the subscriber).
    pub(crate) fn unsubscribe(&self, id: SubscriptionId) {
        self.state
            .lock()
            .expect("mqtt test router mutex poisoned")
            .subscriptions
            .remove(&id);
    }

    /// Fans `payload` out to every subscription on `address` and records it in the published
    /// log. Under a harness run every live enqueue is counted with [`Coordinator::enqueued`].
    pub(crate) fn publish(
        &self,
        address: &str,
        payload: Bytes,
        headers: HeaderMap,
        coordinator: Option<&Coordinator>,
    ) {
        let snapshot = RawMessage::new(address, payload.clone()).with_headers(headers.clone());
        let mut to_notify: Vec<DeliverySender> = Vec::new();
        {
            let mut state = self.state.lock().expect("mqtt test router mutex poisoned");
            state
                .log
                .entry(address.to_owned())
                .or_default()
                .push(snapshot);
            for sub in state.subscriptions.values() {
                if sub.address == address {
                    to_notify.push(sub.sender.clone());
                }
            }
        }

        let delivery = Delivery { payload, headers };
        for tx in to_notify {
            if tx.send(delivery.clone()).is_ok()
                && let Some(coordinator) = coordinator
            {
                coordinator.enqueued();
            }
        }
    }

    /// Returns every message recorded for `address`, in publish order.
    pub(crate) fn published(&self, address: &str) -> Vec<RawMessage> {
        self.state
            .lock()
            .expect("mqtt test router mutex poisoned")
            .log
            .get(address)
            .cloned()
            .unwrap_or_default()
    }

    /// Drops every subscription and clears the published log. Used by broker shutdown.
    pub(crate) fn clear(&self) {
        let mut state = self.state.lock().expect("mqtt test router mutex poisoned");
        state.subscriptions.clear();
        state.log.clear();
    }
}

impl std::fmt::Debug for AddressRouter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let state = self.state.lock().expect("mqtt test router mutex poisoned");
        f.debug_struct("AddressRouter")
            .field("subscriptions", &state.subscriptions.len())
            .field("logged_addresses", &state.log.len())
            .finish_non_exhaustive()
    }
}
