//! The broker's in-process mode, behind the `testing` feature: the transport a connected broker
//! carries when the test harness connects it through `InProcess::connect_in_process` rather than
//! through `connect`.
//!
//! The connected broker, its publishers, its subscribers and its deliveries are the production
//! types, holding this transport as the variant of their link, so a service's descriptors and
//! publish policies run against it unchanged. It has no configuration of its own: it reads the
//! production broker's settings, and a publish goes through the same validation and the same
//! mapping of headers onto MQTT 5 properties as a live one.
//!
//! The transport is a model of the server, and the session's own half is the production code. The
//! server keeps the session's subscriptions, one per wire filter, and the retained message of each
//! topic. A publish reaches every subscription whose filter matches its topic, one packet per
//! subscription at the lesser of the two qualities of service and naming the subscription's
//! identifier, as Mosquitto sends them, and each packet goes through the same demultiplexer the
//! connection task runs. So a share group takes one copy, and two filters overlapping on one topic
//! each receive the packet sent for them. A packet larger than the broker's `max_packet_size` is
//! never sent, as a server discards it. A subscription that is not shared receives the retained
//! messages its filter matches. An acknowledgement after the broker shut down is refused.
//!
//! What belongs to the server and is left to the live mode: the session that outlives a
//! connection (a persistent session, its redelivery of unacknowledged messages and the last will),
//! the receive-maximum window that stalls delivery once enough messages are unacknowledged, the
//! protocol handshakes behind `QoS` 1 and 2, and a server's access control.

use std::collections::{BTreeMap, HashMap};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, OnceLock};

use bytes::Bytes;
use rumqttc::v5::mqttbytes::v5::{Publish, PublishProperties};
use rumqttc::v5::mqttbytes::{QoS, matches};
use ruststream::testing::Coordinator;
use ruststream::{OutgoingMessage, RawMessage};

use crate::broker::Link;
use crate::conn::{Shared, demultiplex};
use crate::error::MqttError;
use crate::filter::Qos;
use crate::message::{MqttMessage, headers_of, to_wire_properties};
use crate::publisher::check_topic;
use crate::registry::SubscribeRequest;

/// The harness's count of one delivery in flight: counted when it is made, released when it is
/// dropped, whether it was settled or not.
pub(crate) struct InFlight(Coordinator);

impl InFlight {
    fn new(coordinator: &Coordinator) -> Self {
        coordinator.enqueued();
        Self(coordinator.clone())
    }
}

impl Drop for InFlight {
    fn drop(&mut self) {
        self.0.consumed();
    }
}

/// One subscription the server holds for this session.
struct ServerSubscription {
    /// The filter as subscribed, `$share/<group>/<filter>` for a share group.
    wire_filter: String,
    /// The filter the topics are matched against.
    filter: String,
    shared: bool,
    qos: QoS,
    /// The subscription identifier the subscribe carried, which every packet sent for this
    /// subscription names.
    identifier: Option<usize>,
}

impl ServerSubscription {
    fn new(wire_filter: String, qos: QoS, identifier: Option<usize>) -> Self {
        let group_filter = wire_filter
            .strip_prefix("$share/")
            .and_then(|rest| rest.split_once('/'))
            .map(|(_, filter)| filter.to_owned());
        Self {
            shared: group_filter.is_some(),
            filter: group_filter.unwrap_or_else(|| wire_filter.clone()),
            wire_filter,
            qos,
            identifier,
        }
    }
}

/// What the server keeps.
#[derive(Default)]
struct Server {
    subscriptions: Vec<ServerSubscription>,
    /// The last retained message of each topic, in topic order.
    retained: BTreeMap<String, Publish>,
    /// Every message published to a topic, as the server received it.
    log: HashMap<String, Vec<RawMessage>>,
}

/// The in-process transport: the server this session is connected to.
pub(crate) struct Bus {
    server: Mutex<Server>,
    coordinator: OnceLock<Coordinator>,
    closed: AtomicBool,
    /// The largest packet the session told the server it accepts, read off the production broker.
    max_packet_size: u32,
}

impl std::fmt::Debug for Bus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Bus")
            .field("max_packet_size", &self.max_packet_size)
            .finish_non_exhaustive()
    }
}

/// Whether a server delivers a message published to `topic` to a subscription on `filter`.
///
/// The protocol's rule, which differs from the client's own match in one place: a topic that
/// starts with `$` is matched by a filter that starts with `$` too, and never by one that starts
/// with a wildcard.
fn server_matches(topic: &str, filter: &str) -> bool {
    topic.strip_prefix('$').map_or_else(
        || matches(topic, filter),
        |topic| {
            filter
                .strip_prefix('$')
                .is_some_and(|filter| matches(topic, filter))
        },
    )
}

/// Names the subscription identifier a packet is sent for among its properties, as a server does:
/// one packet per subscription, carrying that subscription's identifier when it has one.
fn named(packet: &mut Publish, identifier: Option<usize>) {
    if let Some(identifier) = identifier {
        packet
            .properties
            .get_or_insert_with(PublishProperties::default)
            .subscription_identifiers = vec![identifier];
    }
}

/// The lesser of two qualities of service, which is the one a server delivers at.
fn lesser(left: QoS, right: QoS) -> QoS {
    if left <= right { left } else { right }
}

impl Bus {
    pub(crate) fn new(max_packet_size: u32) -> Arc<Self> {
        Arc::new(Self {
            server: Mutex::new(Server::default()),
            coordinator: OnceLock::new(),
            closed: AtomicBool::new(false),
            max_packet_size,
        })
    }

    /// Installs the harness coordinator; a second install is ignored.
    pub(crate) fn install(&self, coordinator: Coordinator) {
        let _ = self.coordinator.set(coordinator);
    }

    /// Whether the session is still open, which is what an acknowledgement needs.
    pub(crate) fn is_open(&self) -> bool {
        !self.closed.load(Ordering::Acquire)
    }

    /// Closes the session, as a `DISCONNECT` does.
    pub(crate) fn close(&self) {
        self.closed.store(true, Ordering::Release);
    }

    fn server(&self) -> std::sync::MutexGuard<'_, Server> {
        self.server
            .lock()
            .expect("mqtt in-process server mutex poisoned")
    }

    /// Subscribes the session as `request` asks, and sends the retained messages the
    /// subscription receives.
    ///
    /// A second subscribe to one wire filter replaces the first, as on a server. A subscribe that
    /// only attaches an identifier to a filter already there asks for no retained messages.
    pub(crate) fn subscribe(self: &Arc<Self>, shared: &Shared, request: &SubscribeRequest) {
        let subscription =
            ServerSubscription::new(request.filter.clone(), request.qos, request.identifier);
        let identifier = subscription.identifier;
        let retained: Vec<Publish> = {
            let mut server = self.server();
            // A share group hands a retained message to none of its members.
            let retained = if subscription.shared || request.refresh {
                Vec::new()
            } else {
                server
                    .retained
                    .iter()
                    .filter(|(topic, _)| server_matches(topic, &subscription.filter))
                    .map(|(_, packet)| {
                        let mut packet = packet.clone();
                        packet.qos = lesser(packet.qos, subscription.qos);
                        named(&mut packet, subscription.identifier);
                        packet
                    })
                    .collect()
            };
            match server
                .subscriptions
                .iter_mut()
                .find(|held| held.wire_filter == subscription.wire_filter)
            {
                Some(held) => *held = subscription,
                None => server.subscriptions.push(subscription),
            }
            retained
        };
        for packet in retained {
            self.send(shared, &packet, identifier);
        }
    }

    /// Drops the session's subscription to `wire_filter`.
    pub(crate) fn unsubscribe(&self, wire_filter: &str) {
        self.server()
            .subscriptions
            .retain(|held| held.wire_filter != wire_filter);
    }

    /// Takes a publish of this session, already validated and mapped onto its packet.
    pub(crate) fn publish(
        self: &Arc<Self>,
        shared: &Shared,
        topic: &str,
        qos: Qos,
        retain: bool,
        payload: Bytes,
        properties: Option<PublishProperties>,
    ) {
        let mut packet = Publish::new(topic, qos.to_client(), payload, properties);
        packet.retain = retain;
        self.receive(shared, &packet);
    }

    /// Takes a publish of another client of the server: at the default quality of service, not
    /// retained, with the headers mapped onto properties as this crate maps them.
    pub(crate) fn inject(
        self: &Arc<Self>,
        shared: &Shared,
        message: &OutgoingMessage<'_>,
    ) -> Result<(), MqttError> {
        check_topic(message.name())?;
        let packet = Publish::new(
            message.name(),
            Qos::default().to_client(),
            Bytes::copy_from_slice(message.payload()),
            to_wire_properties(message),
        );
        self.receive(shared, &packet);
        Ok(())
    }

    /// Every message published to `topic`, in publish order.
    pub(crate) fn published(&self, topic: &str) -> Vec<RawMessage> {
        self.server().log.get(topic).cloned().unwrap_or_default()
    }

    /// What the server does with a PUBLISH it received: logs it, keeps it when it is retained,
    /// and sends it to every subscription of the session its topic matches.
    fn receive(self: &Arc<Self>, shared: &Shared, packet: &Publish) {
        let topic = String::from_utf8_lossy(&packet.topic).into_owned();
        let packets: Vec<(Publish, Option<usize>)> = {
            let mut server = self.server();
            server.log.entry(topic.clone()).or_default().push(
                RawMessage::new(topic.clone(), packet.payload.clone())
                    .with_headers(headers_of(packet)),
            );
            if packet.retain {
                // An empty retained payload is how a publisher clears the topic.
                if packet.payload.is_empty() {
                    server.retained.remove(&topic);
                } else {
                    server.retained.insert(topic.clone(), packet.clone());
                }
            }
            server
                .subscriptions
                .iter()
                .filter(|held| server_matches(&topic, &held.filter))
                .map(|held| {
                    let mut copy = packet.clone();
                    copy.qos = lesser(packet.qos, held.qos);
                    // A subscription that is already there receives the message as published
                    // now, not as the retained one.
                    copy.retain = false;
                    named(&mut copy, held.identifier);
                    (copy, held.identifier)
                })
                .collect()
        };
        for (packet, identifier) in packets {
            self.send(shared, &packet, identifier);
        }
    }

    /// Sends one packet to the session for the subscription carrying `identifier`, which
    /// demultiplexes it as the connection task does.
    fn send(self: &Arc<Self>, shared: &Shared, packet: &Publish, identifier: Option<usize>) {
        let mut sized = packet.clone();
        if sized.qos != QoS::AtMostOnce {
            // A packet id is two bytes of the packet a server sends at `QoS` 1 and 2.
            sized.pkid = 1;
        }
        if sized.size() > self.max_packet_size as usize {
            // The session announced this limit when it connected, and a server discards a message
            // over it rather than sending it.
            tracing::debug!(
                topic = %String::from_utf8_lossy(&packet.topic),
                size = sized.size(),
                max_packet_size = self.max_packet_size,
                "mqtt in-process delivery discarded: larger than the session accepts"
            );
            return;
        }
        let Ok(topic) = std::str::from_utf8(&packet.topic) else {
            return;
        };
        let coordinator = self.coordinator.get();
        let dead = demultiplex(shared, topic, identifier.as_slice(), |acknowledges| {
            MqttMessage::new(
                topic.to_owned(),
                packet,
                acknowledges.then(|| Link::InProcess(Arc::clone(self))),
            )
            .counted(coordinator.map(InFlight::new))
        });
        for member in dead {
            shared.release_with(member, |wire_filter| self.unsubscribe(&wire_filter));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_dollar_topic_is_reached_only_by_a_dollar_filter() {
        assert!(server_matches("$SYS/broker/uptime", "$SYS/#"));
        assert!(!server_matches("$SYS/broker/uptime", "#"));
        assert!(!server_matches("$SYS/broker/uptime", "+/broker/uptime"));
        assert!(server_matches(
            "devices/dev42/telemetry",
            "devices/+/telemetry"
        ));
        assert!(server_matches("devices/dev42", "devices/#"));
        assert!(!server_matches("devices/dev42/telemetry", "devices/+"));
    }

    #[test]
    fn a_share_group_is_one_server_subscription_on_its_plain_filter() {
        let shared =
            ServerSubscription::new("$share/workers/jobs/+".to_owned(), QoS::AtLeastOnce, None);
        assert!(shared.shared);
        assert_eq!(shared.filter, "jobs/+");

        let plain = ServerSubscription::new("jobs/+".to_owned(), QoS::AtLeastOnce, None);
        assert!(!plain.shared);
        assert_eq!(plain.filter, "jobs/+");
    }
}
