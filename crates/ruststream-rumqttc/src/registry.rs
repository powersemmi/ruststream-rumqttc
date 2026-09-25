//! The table of one connection's subscriptions to the server, and the rule that decides which of
//! them a PUBLISH packet belongs to.
//!
//! A subscription to the server is a wire filter (`$share/<group>/<filter>` for a share group).
//! Every local subscription opened on the same wire filter joins it as a member, so the server
//! holds the filter once and the last member to go takes it off the server.
//!
//! When two wire filters on one connection can both match a topic, the server may send a publish
//! once per matching filter, and the topic alone cannot say which copy belongs to which filter.
//! MQTT 5 subscription identifiers can: a wire filter that meets another one carries an identifier,
//! and the server echoes the identifiers of the filters a copy was sent for. A filter that meets no
//! other one carries none, so the common case puts nothing extra on the wire or on a delivery.

use rumqttc::v5::mqttbytes::{QoS, matches};
use tokio::sync::mpsc;

use crate::error::MqttError;
use crate::message::MqttMessage;

/// Where the connection task sends one subscription's deliveries.
pub(crate) type DeliverySender = mpsc::UnboundedSender<Result<MqttMessage, MqttError>>;

/// The largest subscription identifier the protocol can encode (a four-byte variable integer).
const MAX_IDENTIFIER: usize = 268_435_455;

/// One local subscription on a wire filter.
pub(crate) struct Member {
    pub(crate) id: u64,
    pub(crate) qos: QoS,
    pub(crate) tx: DeliverySender,
}

/// One subscription to the server, and the local subscriptions sharing it.
pub(crate) struct Wire {
    /// The filter as subscribed, with its share group when it has one.
    pub(crate) filter: String,
    /// The plain filter a topic is matched against.
    pub(crate) match_filter: String,
    pub(crate) qos: QoS,
    /// Set once this filter meets another one on the connection.
    pub(crate) identifier: Option<usize>,
    /// The server holds this filter without its identifier: it was there before it gained one and
    /// the subscribe attaching it is unanswered, or the server offers no identifiers. A packet it
    /// sends for the filter meanwhile names none.
    unnamed_on_server: bool,
    pub(crate) members: Vec<Member>,
}

/// What a connection must send the server to open one local subscription.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct SubscribeRequest {
    pub(crate) filter: String,
    pub(crate) qos: QoS,
    pub(crate) identifier: Option<usize>,
    /// A filter already on the server that is re-subscribed only to gain its identifier; it asks
    /// for no retained messages, which the first subscribe delivered already.
    pub(crate) refresh: bool,
}

/// What [`Registry::join`] decided.
#[derive(Debug)]
pub(crate) struct Joined {
    pub(crate) member: u64,
    /// In the order they must reach the server: filters gaining an identifier first, the joining
    /// filter last, so the server tells their copies apart before it sends one for the new filter.
    pub(crate) requests: Vec<SubscribeRequest>,
}

/// The subscriptions of one connection.
#[derive(Default)]
pub(crate) struct Registry {
    pub(crate) wires: Vec<Wire>,
    next_member: u64,
    /// Rotates delivery across the members of one wire filter.
    rotation: usize,
}

impl Registry {
    /// Adds a local subscription on `filter`, joining the wire filter when it is already on the
    /// server.
    ///
    /// # Errors
    ///
    /// Refuses a filter that meets a different wire filter of this connection when the server
    /// offers no subscription identifiers: their copies of one publish would be indistinguishable.
    pub(crate) fn join(
        &mut self,
        filter: &str,
        match_filter: &str,
        qos: QoS,
        identifiers_available: bool,
        tx: DeliverySender,
    ) -> Result<Joined, MqttError> {
        let met = self.wires.iter().filter(|wire| {
            wire.filter != filter && filters_overlap(&wire.match_filter, match_filter)
        });
        // Joining a filter already on the server adds no wire subscription, so it meets nothing
        // it did not meet before.
        let exists = self.wires.iter().any(|wire| wire.filter == filter);
        if !identifiers_available
            && !exists
            && let Some(wire) = met.clone().next()
        {
            return Err(MqttError::Subscribe {
                filter: filter.to_owned(),
                reason: format!(
                    "it can match the same topics as '{}' on this connection, and the server does \
                     not offer subscription identifiers, which is what tells their deliveries \
                     apart; open the two on separate connections",
                    wire.filter
                ),
            });
        }
        let meets_another = identifiers_available && met.count() > 0;

        let member = self.next_member;
        self.next_member += 1;
        let mut requests = Vec::new();
        let index = if let Some(index) = self.wires.iter().position(|wire| wire.filter == filter) {
            let wire = &mut self.wires[index];
            wire.members.push(Member {
                id: member,
                qos,
                tx,
            });
            // A subscribe replaces the server's subscription on the same filter, so it asks for
            // the highest quality of service a member needs: a member that acknowledges must
            // never be handed a delivery it cannot acknowledge.
            wire.qos = higher(wire.qos, qos);
            index
        } else {
            self.wires.push(Wire {
                filter: filter.to_owned(),
                match_filter: match_filter.to_owned(),
                qos,
                identifier: None,
                unnamed_on_server: false,
                members: vec![Member {
                    id: member,
                    qos,
                    tx,
                }],
            });
            self.wires.len() - 1
        };
        if meets_another {
            for other in 0..self.wires.len() {
                let wire = &self.wires[other];
                if other != index
                    && wire.identifier.is_none()
                    && filters_overlap(&wire.match_filter, match_filter)
                {
                    let identifier = self.free_identifier(&self.wires[other].filter);
                    let wire = &mut self.wires[other];
                    wire.identifier = Some(identifier);
                    wire.unnamed_on_server = true;
                    requests.push(SubscribeRequest {
                        filter: wire.filter.clone(),
                        qos: wire.qos,
                        identifier: Some(identifier),
                        refresh: true,
                    });
                }
            }
            if self.wires[index].identifier.is_none() {
                let identifier = self.free_identifier(filter);
                self.wires[index].identifier = Some(identifier);
            }
        }
        let wire = &self.wires[index];
        requests.push(SubscribeRequest {
            filter: wire.filter.clone(),
            qos: wire.qos,
            // A server that offers no identifiers may refuse a subscribe carrying one.
            identifier: wire.identifier.filter(|_| identifiers_available),
            refresh: false,
        });
        Ok(Joined { member, requests })
    }

    /// Takes the local subscription `member` out, and answers the wire filter when it was the
    /// last member on it, so the caller unsubscribes it.
    pub(crate) fn leave(&mut self, member: u64) -> Option<String> {
        let index = self
            .wires
            .iter()
            .position(|wire| wire.members.iter().any(|entry| entry.id == member))?;
        let wire = &mut self.wires[index];
        wire.members.retain(|entry| entry.id != member);
        if let Some(qos) = wire.members.iter().map(|entry| entry.qos).reduce(higher) {
            // What a later subscribe of the filter asks for (a reconnect's) follows the members
            // that remain.
            wire.qos = qos;
        }
        wire.members
            .is_empty()
            .then(|| self.wires.swap_remove(index).filter)
    }

    /// Whether the wire filter `filter` is on the server for a local subscription.
    pub(crate) fn holds(&self, filter: &str) -> bool {
        self.wires.iter().any(|wire| wire.filter == filter)
    }

    /// Whether a held delivery naming `identifiers` belongs to the wire filter `filter`: one
    /// naming none belongs to any filter its topic matches, one naming some to the filter they
    /// name, by its identifier now or the one derived from it.
    pub(crate) fn claims(&self, filter: &str, identifiers: &[usize]) -> bool {
        identifiers.is_empty()
            || self
                .wires
                .iter()
                .find(|wire| wire.filter == filter)
                .and_then(|wire| wire.identifier)
                .is_some_and(|identifier| identifiers.contains(&identifier))
            || identifiers.contains(&derived_identifier(filter))
    }

    /// Records the server's answer to the subscribe that attached an identifier to `filter`: from
    /// here on its packets name it, or, when the server refused, they keep naming none.
    pub(crate) fn identified(&mut self, filter: &str, accepted: bool) {
        if let Some(wire) = self.wires.iter_mut().find(|wire| wire.filter == filter) {
            wire.unnamed_on_server = false;
            if !accepted {
                wire.identifier = None;
            }
        }
    }

    /// Records that every filter is being subscribed again on a server that lost the session,
    /// with its identifier when the server offers them.
    pub(crate) fn resubscribing(&mut self, identifiers_available: bool) {
        for wire in &mut self.wires {
            wire.unnamed_on_server = wire.identifier.is_some() && !identifiers_available;
        }
    }

    /// An identifier no wire filter of this connection carries.
    ///
    /// It is derived from the filter, so a process that resumes a persistent session assigns a
    /// filter the identifier the server still holds for it from the previous one, and a packet
    /// queued under the old identifier reaches the filter it was sent for.
    fn free_identifier(&self, filter: &str) -> usize {
        let mut identifier = derived_identifier(filter);
        while self
            .wires
            .iter()
            .any(|wire| wire.identifier == Some(identifier))
        {
            identifier = identifier % MAX_IDENTIFIER + 1;
        }
        identifier
    }

    /// Hands one PUBLISH packet to the subscriptions it belongs to, and answers the members whose
    /// stream is gone. `message` builds one delivery, told whether it is the one carrying the
    /// acknowledgement.
    ///
    /// A packet naming identifiers belongs to the wire filters carrying them; one naming none
    /// belongs to the matching filters the server holds without one (including a filter whose
    /// identifier is still on its way to the server). Each such filter receives one copy,
    /// rotated across its members, and the first copy carries the acknowledgement. A packet the
    /// rule leaves with no filter goes, when it names identifiers, to the matching filter whose
    /// derived identifier it names (a filter a previous process identified that carries none
    /// now), and is otherwise answered back to be held for the filter it names, which has not
    /// opened yet; one naming none goes to every filter its topic matches. A packet no filter
    /// matches at all is answered back, for the caller to hold.
    pub(crate) fn route(
        &mut self,
        topic: &str,
        identifiers: &[usize],
        mut message: impl FnMut(bool) -> MqttMessage,
        dead: &mut Vec<u64>,
    ) -> Option<MqttMessage> {
        let rotation = self.rotation;
        self.rotation = self.rotation.wrapping_add(1);
        let mut acknowledges = true;
        let mut deliver = |wire: &Wire| {
            let member = &wire.members[rotation % wire.members.len()];
            if member
                .tx
                .send(Ok(message(std::mem::take(&mut acknowledges))))
                .is_err()
            {
                dead.push(member.id);
            }
        };
        let mut delivered = false;
        for wire in &self.wires {
            let named = if identifiers.is_empty() {
                wire.identifier.is_none() || wire.unnamed_on_server
            } else {
                wire.identifier
                    .is_some_and(|identifier| identifiers.contains(&identifier))
            };
            if named && matches(topic, &wire.match_filter) {
                deliver(wire);
                delivered = true;
            }
        }
        if !delivered {
            for wire in &self.wires {
                let named = identifiers.is_empty()
                    || (wire.identifier.is_none()
                        && identifiers.contains(&derived_identifier(&wire.filter)));
                if named && matches(topic, &wire.match_filter) {
                    deliver(wire);
                    delivered = true;
                }
            }
        }
        (!delivered).then(|| message(true))
    }
}

/// The higher of two qualities of service.
const fn higher(left: QoS, right: QoS) -> QoS {
    if (left as u8) >= (right as u8) {
        left
    } else {
        right
    }
}

/// The identifier a filter is given first: derived from the filter, so a process that resumes a
/// persistent session assigns it the identifier the server still holds from the previous one.
fn derived_identifier(filter: &str) -> usize {
    // FNV-1a: stable across processes and releases, unlike the standard library's hasher.
    let hash = filter
        .bytes()
        .fold(0xcbf2_9ce4_8422_2325_u64, |hash, byte| {
            (hash ^ u64::from(byte)).wrapping_mul(0x0100_0000_01b3)
        });
    let range = MAX_IDENTIFIER as u64;
    usize::try_from(hash % range).unwrap_or(0) + 1
}

/// Whether some topic matches both filters.
pub(crate) fn filters_overlap(left: &str, right: &str) -> bool {
    let mut left_levels = left.split('/');
    let mut right_levels = right.split('/');
    let mut first = true;
    loop {
        match (left_levels.next(), right_levels.next()) {
            (None | Some("#"), None) | (None, Some("#")) => return true,
            (None, Some(_)) | (Some(_), None) => return false,
            (Some(left), Some(right)) => {
                let left_wild = left == "+" || left == "#";
                let right_wild = right == "+" || right == "#";
                // A wildcard at the first level does not match a topic starting with `$`.
                if first
                    && ((left_wild && right.starts_with('$'))
                        || (right_wild && left.starts_with('$')))
                {
                    return false;
                }
                if left == "#" || right == "#" {
                    return true;
                }
                if !left_wild && !right_wild && left != right {
                    return false;
                }
            }
        }
        first = false;
    }
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;
    use rumqttc::v5::mqttbytes::v5::{Publish, PublishProperties};
    use tokio::sync::mpsc::UnboundedReceiver;

    use super::*;

    type Receiver = UnboundedReceiver<Result<MqttMessage, MqttError>>;

    fn join(
        registry: &mut Registry,
        filter: &str,
        match_filter: &str,
        available: bool,
    ) -> Result<(Joined, Receiver), MqttError> {
        let (tx, rx) = mpsc::unbounded_channel();
        registry
            .join(filter, match_filter, QoS::AtLeastOnce, available, tx)
            .map(|joined| (joined, rx))
    }

    fn packet(topic: &str, identifiers: Vec<usize>) -> Publish {
        let mut publish = Publish::new(topic, QoS::AtLeastOnce, Bytes::from_static(b"x"), None);
        if !identifiers.is_empty() {
            publish.properties = Some(PublishProperties {
                subscription_identifiers: identifiers,
                ..PublishProperties::default()
            });
        }
        publish
    }

    fn route(registry: &mut Registry, publish: &Publish) -> Option<MqttMessage> {
        let topic = std::str::from_utf8(&publish.topic).expect("utf-8 topic");
        let identifiers = publish
            .properties
            .as_ref()
            .map_or(&[][..], |properties| &properties.subscription_identifiers);
        let mut dead = Vec::new();
        let unmatched = registry.route(
            topic,
            identifiers,
            |_| MqttMessage::new(topic.to_owned(), publish, None),
            &mut dead,
        );
        assert!(dead.is_empty(), "every member is alive");
        unmatched
    }

    fn drain(rx: &mut Receiver) -> usize {
        let mut count = 0;
        while rx.try_recv().is_ok() {
            count += 1;
        }
        count
    }

    fn identifier(registry: &Registry, filter: &str) -> Option<usize> {
        registry
            .wires
            .iter()
            .find(|wire| wire.filter == filter)
            .and_then(|wire| wire.identifier)
    }

    #[test]
    fn filters_overlap_when_some_topic_matches_both() {
        for (left, right, expected) in [
            ("a/b", "a/b", true),
            ("a/b", "a/c", false),
            ("a/+", "a/b", true),
            ("a/+", "a/#", true),
            ("a/#", "a", true),
            ("a/+", "a", false),
            ("a/+/c", "a/b/d", false),
            ("+/+", "a/+/c", false),
            ("#", "anything/at/all", true),
            ("#", "$SYS/uptime", false),
            ("+/uptime", "$SYS/uptime", false),
            ("$SYS/#", "$SYS/uptime", true),
            ("a/b/c", "a/+/c", true),
        ] {
            assert_eq!(filters_overlap(left, right), expected, "{left} and {right}");
            assert_eq!(filters_overlap(right, left), expected, "{right} and {left}");
        }
    }

    #[test]
    fn a_filter_that_meets_no_other_carries_no_identifier() {
        let mut registry = Registry::default();
        let (first, _a) = join(&mut registry, "a/b", "a/b", true).expect("joins");
        let (second, _b) = join(&mut registry, "c/+", "c/+", true).expect("joins");

        assert_eq!(first.requests[0].identifier, None);
        assert_eq!(second.requests.len(), 1, "nothing is re-subscribed");
        assert_eq!(second.requests[0].identifier, None);
    }

    #[test]
    fn meeting_filters_both_gain_an_identifier_the_older_one_first() {
        let mut registry = Registry::default();
        join(&mut registry, "a/+", "a/+", true).expect("joins");
        let (joined, _rx) = join(&mut registry, "a/#", "a/#", true).expect("joins");

        let older = identifier(&registry, "a/+").expect("the older filter gained one");
        let newer = identifier(&registry, "a/#").expect("the new filter carries one");
        assert_ne!(older, newer);
        assert_eq!(
            joined.requests,
            vec![
                SubscribeRequest {
                    filter: "a/+".to_owned(),
                    qos: QoS::AtLeastOnce,
                    identifier: Some(older),
                    refresh: true,
                },
                SubscribeRequest {
                    filter: "a/#".to_owned(),
                    qos: QoS::AtLeastOnce,
                    identifier: Some(newer),
                    refresh: false,
                },
            ]
        );
    }

    #[test]
    fn a_packet_reaches_only_the_filters_its_identifiers_name() {
        let mut registry = Registry::default();
        let (_, mut single) = join(&mut registry, "a/+", "a/+", true).expect("joins");
        let (_, mut every) = join(&mut registry, "a/#", "a/#", true).expect("joins");
        let single_id = identifier(&registry, "a/+").expect("identified");
        let every_id = identifier(&registry, "a/#").expect("identified");

        // The server sends one copy per matching filter, each naming its own.
        assert!(route(&mut registry, &packet("a/x", vec![single_id])).is_none());
        assert!(route(&mut registry, &packet("a/x", vec![every_id])).is_none());
        assert_eq!((drain(&mut single), drain(&mut every)), (1, 1));

        // Or one copy naming both.
        assert!(route(&mut registry, &packet("a/x", vec![single_id, every_id])).is_none());
        assert_eq!((drain(&mut single), drain(&mut every)), (1, 1));
    }

    #[test]
    fn a_packet_naming_nothing_reaches_the_filters_that_carry_nothing() {
        let mut registry = Registry::default();
        let (_, mut plain) = join(&mut registry, "a/b", "a/b", true).expect("joins");
        let (_, mut other) = join(&mut registry, "c/d", "c/d", true).expect("joins");

        assert!(route(&mut registry, &packet("a/b", vec![])).is_none());
        assert_eq!((drain(&mut plain), drain(&mut other)), (1, 0));
    }

    #[test]
    fn a_filter_gaining_an_identifier_keeps_packets_naming_none_until_the_server_answers() {
        let mut registry = Registry::default();
        let (_, mut single) = join(&mut registry, "a/+", "a/+", true).expect("joins");
        let (_, mut every) = join(&mut registry, "a/#", "a/#", true).expect("joins");

        // Sent before the server attached the identifier: the older filter's alone.
        assert!(route(&mut registry, &packet("a/x", vec![])).is_none());
        assert_eq!((drain(&mut single), drain(&mut every)), (1, 0));

        registry.identified("a/+", true);
        let single_id = identifier(&registry, "a/+").expect("identified");
        assert!(route(&mut registry, &packet("a/x", vec![single_id])).is_none());
        assert_eq!((drain(&mut single), drain(&mut every)), (1, 0));
    }

    #[test]
    fn a_refused_identifier_leaves_the_filter_without_one() {
        let mut registry = Registry::default();
        join(&mut registry, "a/+", "a/+", true).expect("joins");
        join(&mut registry, "a/#", "a/#", true).expect("joins");

        registry.identified("a/+", false);
        assert_eq!(identifier(&registry, "a/+"), None);
    }

    #[test]
    fn an_identifier_a_previous_process_gave_a_filter_still_reaches_it() {
        let mut registry = Registry::default();
        let (_, mut plain) = join(&mut registry, "a/b", "a/b", true).expect("joins");

        let previous = derived_identifier("a/b");
        assert!(route(&mut registry, &packet("a/b", vec![previous])).is_none());
        assert_eq!(drain(&mut plain), 1);
    }

    #[test]
    fn a_packet_for_a_filter_not_open_yet_is_held_for_it() {
        let mut registry = Registry::default();
        let (_, mut open) = join(&mut registry, "a/+", "a/+", true).expect("joins");

        let later = derived_identifier("a/#");
        assert!(
            route(&mut registry, &packet("a/b", vec![later])).is_some(),
            "held for the filter it names"
        );
        assert_eq!(
            drain(&mut open),
            0,
            "an overlapping filter does not take it"
        );
        assert!(!registry.claims("a/+", &[later]));
        join(&mut registry, "a/#", "a/#", true).expect("joins");
        assert!(registry.claims("a/#", &[later]));
    }

    #[test]
    fn a_member_keeps_the_quality_of_service_it_asked_for() {
        let mut registry = Registry::default();
        let (_, _acknowledging) = join(&mut registry, "a/b", "a/b", true).expect("joins");
        let (fire_tx, _fire_rx) = mpsc::unbounded_channel::<Result<MqttMessage, MqttError>>();
        let fire = registry
            .join("a/b", "a/b", QoS::AtMostOnce, true, fire_tx)
            .expect("joins");
        assert_eq!(
            fire.requests[0].qos,
            QoS::AtLeastOnce,
            "the highest member's"
        );
        registry.leave(fire.member);
        assert_eq!(registry.wires[0].qos, QoS::AtLeastOnce);
    }

    #[test]
    fn a_member_joins_its_filter_without_identifiers_and_sends_none() {
        let mut registry = Registry::default();
        join(&mut registry, "a/+", "a/+", true).expect("joins");
        join(&mut registry, "a/#", "a/#", true).expect("joins");
        let (joined, _rx) =
            join(&mut registry, "a/+", "a/+", false).expect("an existing filter gains a member");
        assert_eq!(joined.requests.len(), 1, "nothing is re-subscribed");
        assert_eq!(joined.requests[0].identifier, None);
    }

    #[test]
    fn a_packet_no_filter_matches_is_answered_back() {
        let mut registry = Registry::default();
        let (_, mut plain) = join(&mut registry, "a/b", "a/b", true).expect("joins");

        assert!(route(&mut registry, &packet("z", vec![])).is_some());
        assert_eq!(drain(&mut plain), 0);
    }

    #[test]
    fn members_of_one_filter_take_turns() {
        let mut registry = Registry::default();
        let (_, mut first) = join(&mut registry, "a/b", "a/b", true).expect("joins");
        let (_, mut second) = join(&mut registry, "a/b", "a/b", true).expect("joins");

        route(&mut registry, &packet("a/b", vec![]));
        route(&mut registry, &packet("a/b", vec![]));
        assert_eq!((drain(&mut first), drain(&mut second)), (1, 1));
    }

    #[test]
    fn the_last_member_to_leave_takes_the_filter_off_the_server() {
        let mut registry = Registry::default();
        let (first, _a) = join(&mut registry, "a/b", "a/b", true).expect("joins");
        let (second, _b) = join(&mut registry, "a/b", "a/b", true).expect("joins");

        assert_eq!(registry.leave(first.member), None, "a member remains");
        assert_eq!(registry.leave(second.member), Some("a/b".to_owned()));
        assert!(registry.wires.is_empty());
    }

    #[test]
    fn meeting_filters_are_refused_without_identifiers() {
        let mut registry = Registry::default();
        join(&mut registry, "a/+", "a/+", false).expect("joins");
        join(&mut registry, "a/+", "a/+", false).expect("the same filter joins");
        join(&mut registry, "b/c", "b/c", false).expect("a filter meeting none joins");

        let error = join(&mut registry, "$share/g/a/x", "a/x", false)
            .expect_err("a share group on a matching filter meets it");
        assert!(
            matches!(&error, MqttError::Subscribe { reason, .. } if reason.contains("'a/+'")),
            "the refusal names the filter it meets: {error}"
        );
    }

    #[test]
    fn identifiers_are_derived_from_the_filter() {
        let mut first = Registry::default();
        join(&mut first, "a/+", "a/+", true).expect("joins");
        join(&mut first, "a/#", "a/#", true).expect("joins");
        let mut second = Registry::default();
        join(&mut second, "a/#", "a/#", true).expect("joins");
        join(&mut second, "a/+", "a/+", true).expect("joins");

        for filter in ["a/+", "a/#"] {
            assert_eq!(
                identifier(&first, filter),
                identifier(&second, filter),
                "{filter}"
            );
        }
    }
}
