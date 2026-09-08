//! Subscription registry and fanout for the in-process MQTT stand-in.
//!
//! Routing only: a published topic fans out to every live subscription whose filter matches it,
//! and a per-topic log records traffic for assertions. The match is
//! [`rumqttc`'s own](rumqttc::v5::mqttbytes::matches), the function the connection task
//! demultiplexes incoming packets with, so a filter selects the same topics here as on the wire
//! and a wildcard descriptor does not need a rewritten name to be testable.
//!
//! Subscriptions in one share group take turns instead of each taking a copy, because competing
//! consumers are the whole of what the group is for: a stand-in that handed the message to every
//! member would let a test claim work was shared while both members did it. Members are grouped by
//! the wire filter, `$share/<group>/<filter>`, so the same filter in two groups is two
//! independent groups, and a subscription outside a group still gets its own copy - two service
//! instances subscribing plainly are two clients on a server, not one.
//!
//! A delivery carries the quality of service its publish asked for, so the subscriber can settle
//! it the way the wire would. What is not simulated is the protocol behind that level - the
//! acknowledgement exchange itself, retained messages, and the session that redelivers.

use std::collections::HashMap;
use std::sync::{
    Mutex,
    atomic::{AtomicU64, Ordering},
};

use bytes::Bytes;
use rumqttc::v5::mqttbytes::matches;
use ruststream::{HeaderMap, RawMessage, testing::Coordinator};
use tokio::sync::mpsc;

use crate::filter::Qos;

/// Opaque handle identifying one subscription inside an [`AddressRouter`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) struct SubscriptionId(u64);

/// Single delivery handed to a matching subscriber.
#[derive(Debug, Clone)]
pub(crate) struct Delivery {
    pub(crate) payload: Bytes,
    pub(crate) headers: HeaderMap,
    /// The quality of service the publish asked for. The subscription's own caps it, the way the
    /// wire delivers at the lesser of the two.
    pub(crate) qos: Qos,
}

pub(crate) type DeliverySender = mpsc::UnboundedSender<Delivery>;
pub(crate) type DeliveryReceiver = mpsc::UnboundedReceiver<Delivery>;

struct Subscription {
    filter: String,
    /// The share group's wire filter, when this subscription is in one. Members sharing it take
    /// one delivery between them.
    group: Option<String>,
    sender: DeliverySender,
}

#[derive(Default)]
struct RouterState {
    subscriptions: HashMap<SubscriptionId, Subscription>,
    log: HashMap<String, Vec<RawMessage>>,
}

/// In-memory router: topic filters in, published topics out.
#[derive(Default)]
pub(crate) struct AddressRouter {
    state: Mutex<RouterState>,
    next_id: AtomicU64,
    /// Rotates delivery across the members of a share group.
    round_robin: AtomicU64,
}

impl AddressRouter {
    /// Registers a subscription on `filter`, in the share group `group` names, and returns the
    /// channel pair the subscriber will use, together with the [`SubscriptionId`] needed to
    /// unsubscribe.
    ///
    /// The returned [`DeliverySender`] is the same one fanout uses, so subscribers can re-send
    /// a delivery into their own queue to implement `nack(requeue = true)`.
    pub(crate) fn subscribe(
        &self,
        filter: String,
        group: Option<String>,
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
                    filter,
                    group,
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

    /// Fans `payload` out to every subscription whose filter matches `address` - one copy each,
    /// except that a share group takes one copy between its members - and records it in the
    /// published log, which is keyed by the topic as published. Under a harness run every live
    /// enqueue is counted with [`Coordinator::enqueued`].
    pub(crate) fn publish(
        &self,
        address: &str,
        payload: Bytes,
        headers: HeaderMap,
        qos: Qos,
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
            let mut groups: Vec<(&str, Vec<&DeliverySender>)> = Vec::new();
            for sub in state.subscriptions.values() {
                if !matches(address, &sub.filter) {
                    continue;
                }
                match &sub.group {
                    None => to_notify.push(sub.sender.clone()),
                    Some(group) => match groups.iter_mut().find(|(key, _)| *key == group) {
                        Some((_, members)) => members.push(&sub.sender),
                        None => groups.push((group, vec![&sub.sender])),
                    },
                }
            }
            if !groups.is_empty() {
                let turn =
                    usize::try_from(self.round_robin.fetch_add(1, Ordering::Relaxed)).unwrap_or(0);
                for (_, members) in &groups {
                    to_notify.push(members[turn % members.len()].clone());
                }
            }
            drop(state);
        }

        let delivery = Delivery {
            payload,
            headers,
            qos,
        };
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

#[cfg(test)]
mod tests {
    use super::*;

    fn publish(router: &AddressRouter, topic: &str) {
        router.publish(
            topic,
            Bytes::from_static(b"{}"),
            HeaderMap::new(),
            Qos::AtLeastOnce,
            None,
        );
    }

    #[test]
    fn a_wildcard_filter_selects_the_topics_it_would_select_on_the_wire() {
        let router = AddressRouter::default();
        let (_id, _requeue, mut rx) = router.subscribe("devices/+/telemetry".to_owned(), None);

        publish(&router, "devices/dev42/telemetry");
        publish(&router, "devices/dev42/state");
        publish(&router, "devices/dev42/telemetry/raw");

        assert!(rx.try_recv().is_ok(), "the matching topic is delivered");
        assert!(
            rx.try_recv().is_err(),
            "a topic the filter does not cover is not"
        );
    }

    #[test]
    fn a_terminal_hash_covers_every_level_below_it() {
        let router = AddressRouter::default();
        let (_id, _requeue, mut rx) = router.subscribe("devices/#".to_owned(), None);

        publish(&router, "devices/dev42/telemetry/raw");
        publish(&router, "sensors/dev42/telemetry");

        assert!(rx.try_recv().is_ok());
        assert!(rx.try_recv().is_err());
    }

    #[test]
    fn the_published_log_is_keyed_by_the_topic_not_the_filter() {
        let router = AddressRouter::default();
        let (_id, _requeue, _rx) = router.subscribe("devices/+/telemetry".to_owned(), None);

        publish(&router, "devices/dev42/telemetry");

        assert_eq!(router.published("devices/dev42/telemetry").len(), 1);
        assert!(
            router.published("devices/+/telemetry").is_empty(),
            "assertions address the topic a producer published, which is never a filter"
        );
    }

    #[test]
    fn one_share_group_takes_one_copy_between_its_members() {
        let router = AddressRouter::default();
        let group = Some("$share/workers/jobs".to_owned());
        let (_a, _ra, mut first) = router.subscribe("jobs".to_owned(), group.clone());
        let (_b, _rb, mut second) = router.subscribe("jobs".to_owned(), group);

        for _ in 0..4 {
            publish(&router, "jobs");
        }

        let mut delivered = 0;
        while first.try_recv().is_ok() {
            delivered += 1;
        }
        while second.try_recv().is_ok() {
            delivered += 1;
        }
        assert_eq!(
            delivered, 4,
            "four publishes are four deliveries across the group, not eight"
        );
    }

    #[test]
    fn two_groups_on_one_filter_each_take_their_own_copy() {
        let router = AddressRouter::default();
        let (_a, _ra, mut workers) =
            router.subscribe("jobs".to_owned(), Some("$share/workers/jobs".to_owned()));
        let (_b, _rb, mut auditors) =
            router.subscribe("jobs".to_owned(), Some("$share/auditors/jobs".to_owned()));

        publish(&router, "jobs");

        assert!(workers.try_recv().is_ok());
        assert!(
            auditors.try_recv().is_ok(),
            "a second group is a second subscription on the server, not a competitor"
        );
    }

    #[test]
    fn plain_subscriptions_on_one_filter_each_take_a_copy() {
        let router = AddressRouter::default();
        let (_a, _ra, mut first) = router.subscribe("jobs".to_owned(), None);
        let (_b, _rb, mut second) = router.subscribe("jobs".to_owned(), None);

        publish(&router, "jobs");

        assert!(first.try_recv().is_ok());
        assert!(
            second.try_recv().is_ok(),
            "outside a group every subscriber gets its own copy"
        );
    }
}
