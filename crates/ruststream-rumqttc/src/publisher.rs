//! [`MqttPublisher`], its [`MqttPublish`] policy, and the per-message settings.

// Without the `testing` feature a link has one variant, so a `match` on it has a single arm; the
// match stays so that the in-process arm has its place when the feature is on.
#![cfg_attr(
    not(feature = "testing"),
    allow(clippy::infallible_destructuring_match)
)]

use std::fmt;
use std::future::{Future, ready};

use bytes::Bytes;
use rumqttc::v5::mqttbytes::v5::PublishProperties;
use rumqttc::v5::mqttbytes::valid_topic;
#[cfg(feature = "asyncapi")]
use ruststream::asyncapi::Bindings;
use ruststream::runtime::{PublishBuilder, PublishSink};
use ruststream::{BytesMut, OutgoingMessage, PairError, PublishPolicy, Publisher, Take};

#[cfg(feature = "asyncapi")]
use crate::asyncapi;
use crate::broker::{ConnectedMqttBroker, CoreCell, Link};
use crate::error::MqttError;
use crate::filter::Qos;
use crate::message::to_wire_properties;

/// Refuses a topic no server takes, before any I/O.
///
/// The client's send-path error cannot say why a request failed, so the topic is validated here; a
/// remaining failure unambiguously means the connection is gone. The client passes an empty topic
/// through, and a server answers that packet by closing the session, which would take every other
/// message in flight with it.
pub(crate) fn check_topic(topic: &str) -> Result<(), MqttError> {
    let reason = if topic.is_empty() {
        "an MQTT topic must not be empty"
    } else if !valid_topic(topic) {
        "not a valid MQTT topic (wildcards are subscribe-only)"
    } else {
        return Ok(());
    };
    Err(MqttError::Publish {
        topic: topic.to_owned(),
        reason: reason.to_owned(),
    })
}

/// The single send path: every publishing form resolves to a `QoS` and a retain flag, and the
/// wire work happens here once.
async fn send(
    cell: &CoreCell,
    qos: Qos,
    retain: bool,
    msg: OutgoingMessage<'_, BytesMut>,
) -> Result<(), MqttError> {
    let core = cell.get().ok_or(MqttError::NotConnected)?;
    core.shared.ensure_open()?;
    check_topic(msg.name())?;
    let topic = msg.name();
    let (payload, properties) = into_packet(msg);
    let client = match &core.link {
        Link::Wire(client) => client,
        #[cfg(feature = "testing")]
        Link::InProcess(bus) => {
            bus.publish(&core.shared, topic, qos, retain, payload, properties);
            return Ok(());
        }
    };
    let outcome = match properties {
        Some(properties) => {
            client
                .publish_bytes_with_properties(topic, qos.to_client(), retain, payload, properties)
                .await
        }
        None => {
            client
                .publish_bytes(topic, qos.to_client(), retain, payload)
                .await
        }
    };
    outcome.map_err(|_| MqttError::Publish {
        topic: topic.to_owned(),
        reason: "the mqtt connection task has shut down".to_owned(),
    })
}

/// What a PUBLISH packet carries, taken out of the message in one move: the payload the session
/// keeps and the MQTT 5 properties the headers map onto.
fn into_packet(msg: OutgoingMessage<'_, BytesMut>) -> (Bytes, Option<PublishProperties>) {
    let properties = to_wire_properties(&msg);
    (msg.into_payload().freeze(), properties)
}

/// Publishes messages to MQTT topics through the shared connection.
///
/// The publish is queued into the client session: for `QoS` 1/2 the session's state machine
/// retransmits until the broker acknowledges (surviving reconnects), so `Ok` means "owned by
/// the session", not "broker confirmed". Buildable before `connect` and usable until
/// `shutdown`; afterwards every publish reports [`MqttError::NotConnected`].
#[derive(Clone)]
pub struct MqttPublisher {
    cell: CoreCell,
    qos: Qos,
    retain: bool,
}

impl fmt::Debug for MqttPublisher {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("MqttPublisher")
            .field("qos", &self.qos)
            .field("retain", &self.retain)
            .finish_non_exhaustive()
    }
}

impl MqttPublisher {
    pub(crate) fn new(cell: CoreCell, qos: Qos, retain: bool) -> Self {
        Self { cell, qos, retain }
    }
}

impl Publisher for MqttPublisher {
    /// The client keeps the payload: a PUBLISH packet is queued into the session, which holds
    /// the `Bytes` until the broker acknowledges it.
    type Payload = Take;

    type Error = MqttError;
    type Options = MqttPublishOptions;

    async fn publish(
        &self,
        msg: OutgoingMessage<'_, BytesMut>,
        options: Option<&Self::Options>,
    ) -> Result<(), Self::Error> {
        let (qos, retain) = MqttPublishOptions::resolve(options, self.qos, self.retain);
        send(&self.cell, qos, retain, msg).await
    }
}

/// The two arguments MQTT carries on every PUBLISH packet, as one message asked for them.
///
/// This is [`Publisher::Options`] for every publisher this crate hands out. An argument left
/// unset keeps what the [`MqttPublish`] policy fixed at the mount site, so a call carries only
/// what it changed, and a path with no call site at all - a reply, the runtime's deferred
/// redelivery - carries nothing and publishes entirely under the policy.
///
/// A service writes [the steps](MqttPublishSteps) rather than this type; it is named in a test
/// asserting what a publish carried (`with_options`) and in the
/// `Out<impl Publisher<Options = MqttPublishOptions>, Marker>` bound of a handler body that takes
/// one.
///
/// # Examples
///
/// ```
/// use ruststream_rumqttc::{MqttPublishOptions, Qos};
///
/// let options = MqttPublishOptions::default().qos(Qos::ExactlyOnce).retain(true);
/// # let _ = options;
/// ```
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
#[must_use]
pub struct MqttPublishOptions {
    qos: Option<Qos>,
    retain: Option<bool>,
}

impl MqttPublishOptions {
    /// Sends this one message at `qos` instead of the policy's.
    pub const fn qos(mut self, qos: Qos) -> Self {
        self.qos = Some(qos);
        self
    }

    /// Sends this one message retained (or explicitly not retained), whatever the policy declares.
    ///
    /// A retained message is the last one the broker keeps per topic and hands to each new
    /// subscriber on a matching filter; publishing an empty payload retained clears it.
    pub const fn retain(mut self, retain: bool) -> Self {
        self.retain = Some(retain);
        self
    }

    /// Resolves one call's arguments over the defaults its publisher was paired with.
    pub(crate) fn resolve(options: Option<&Self>, qos: Qos, retain: bool) -> (Qos, bool) {
        let options = options.copied().unwrap_or_default();
        (options.qos.unwrap_or(qos), options.retain.unwrap_or(retain))
    }
}

/// The per-message steps of an MQTT publish, on the publish builder itself.
///
/// Both arguments MQTT carries on a PUBLISH packet are reopened at the call site:
/// `out.message(&state).retain(true).publish()`. The steps sit on the builder rather than
/// wrapping the publisher, so the publish still leaves through the mount site's own entry - with
/// the codec that entry named, and attributed to the slot it belongs to.
///
/// The bound is on the sink's [options type](Publisher::Options), so these steps appear on a
/// builder over an MQTT publisher and on no other broker's. A handler body that takes one imports
/// this crate's prelude and bounds its slot with
/// `Out<impl Publisher<Options = MqttPublishOptions>, Marker>`; everywhere else - a startup hook,
/// a test - the steps are already there on the builder the publisher hands out.
///
/// # Examples
///
/// ```
/// use ruststream::runtime::PublishExt;
/// use ruststream::{Outgoing, Serialized};
/// use ruststream_rumqttc::prelude::*;
///
/// // An MQTT state is bytes on the wire rather than an encoded model, so the type carries its
/// // own bytes and no codec runs on them.
/// #[derive(Outgoing, Serialized)]
/// #[outgoing(name = "devices/dev42/state")]
/// struct DeviceState(Vec<u8>);
///
/// # async fn demo() -> Result<(), Box<dyn std::error::Error>> {
/// let publisher = MqttBroker::new("mqtt://localhost:1883", "states").publisher();
/// publisher
///     .message(&DeviceState(b"online".to_vec()))
///     .retain(true)
///     .qos(Qos::ExactlyOnce)
///     .publish()
///     .await?;
/// # Ok(())
/// # }
/// ```
pub trait MqttPublishSteps {
    /// Sends this one message at `qos` instead of the publisher's own.
    #[must_use]
    fn qos(self, qos: Qos) -> Self;

    /// Sends this one message retained (or explicitly not retained), whatever the publisher's
    /// policy declares.
    #[must_use]
    fn retain(self, retain: bool) -> Self;
}

impl<Sink, Body, Enc, Hdrs, Dest> MqttPublishSteps for PublishBuilder<Sink, Body, Enc, Hdrs, Dest>
where
    Sink: PublishSink<Options = MqttPublishOptions>,
{
    fn qos(mut self, qos: Qos) -> Self {
        self.options_mut()
            .get_or_insert_with(MqttPublishOptions::default)
            .qos = Some(qos);
        self
    }

    fn retain(mut self, retain: bool) -> Self {
        self.options_mut()
            .get_or_insert_with(MqttPublishOptions::default)
            .retain = Some(retain);
        self
    }
}

/// The publish policy for [`MqttPublisher`]: quality of service and the retain flag as pure
/// declaration, paired with the connected broker by the runtime after `connect`.
///
/// # Examples
///
/// ```
/// use ruststream_rumqttc::{MqttPublish, Qos};
///
/// let policy = MqttPublish::default().qos(Qos::ExactlyOnce).retain(true);
/// # let _ = policy;
/// ```
#[derive(Debug, Clone, Copy, Default)]
#[must_use]
pub struct MqttPublish {
    qos: Qos,
    retain: bool,
}

impl MqttPublish {
    /// Sets the delivery quality of service. Defaults to [`Qos::AtLeastOnce`].
    pub fn qos(mut self, qos: Qos) -> Self {
        self.qos = qos;
        self
    }

    /// Publishes messages as retained: the broker keeps the last one per topic and hands it
    /// to new (non-shared) subscribers.
    pub fn retain(mut self, retain: bool) -> Self {
        self.retain = retain;
        self
    }
}

impl MqttPublish {
    pub(crate) fn into_publisher(self, cell: CoreCell) -> MqttPublisher {
        MqttPublisher::new(cell, self.qos, self.retain)
    }
}

impl PublishPolicy<ConnectedMqttBroker> for MqttPublish {
    type Live = MqttPublisher;

    fn pair(
        self,
        connected: &ConnectedMqttBroker,
    ) -> impl Future<Output = Result<Self::Live, PairError>> {
        ready(Ok(connected.publisher_with(self)))
    }

    /// What every packet this policy sends carries: the quality of service and the retain flag,
    /// which the `mqtt` binding puts on the operation.
    #[cfg(feature = "asyncapi")]
    fn operation_bindings(&self, _channel: &str) -> Bindings {
        asyncapi::send_operation(self.qos, self.retain)
    }

    /// The MQTT 5 properties every message this policy sends is mapped through.
    ///
    /// The destination the mount site resolved is not read here, nor on the operation: the `mqtt`
    /// binding has no field that names one. The Response Topic property is the requester's to
    /// set, so a message a service sends describes none, and where a reply goes is the
    /// operation's `reply` object.
    #[cfg(feature = "asyncapi")]
    fn message_bindings(&self, _channel: &str) -> Bindings {
        asyncapi::publish_message()
    }

    /// The crate answers a request through the Response Topic property, which arrives as the
    /// `reply-to` header, so that is where a client reads the address of its answer.
    #[cfg(feature = "asyncapi")]
    fn reply_address_location(&self) -> Option<&'static str> {
        Some(asyncapi::REPLY_ADDRESS_LOCATION)
    }
}

#[cfg(test)]
mod tests {
    use ruststream::runtime::PublishExt;
    use ruststream::{HeaderMap, Outgoing, Serialized};

    use super::*;
    use crate::broker::MqttBroker;

    /// The packet is made of the buffer the publish wrote, not of a copy of it: the session keeps
    /// the payload, so this crate hands it over.
    #[test]
    fn the_packet_carries_the_buffer_that_was_handed_in() {
        let body = BytesMut::from(&br#"{"id":7}"#[..]);
        let written_at = body.as_ptr();
        let mut headers = HeaderMap::new();
        headers.insert("content-type", "application/json");

        let (payload, properties) =
            into_packet(OutgoingMessage::produced("orders", body).with_headers(headers));

        assert!(properties.is_some(), "the headers still map to properties");
        assert_eq!(
            payload.as_ptr(),
            written_at,
            "the packet must carry the buffer the publish wrote, not a copy of it",
        );
    }

    fn publisher() -> MqttPublisher {
        MqttBroker::new("mqtt://localhost:1883", "arguments").publisher()
    }

    /// A payload that carries its own bytes, so a builder can be assembled without a codec in
    /// the picture: what these tests read is the options the steps wrote, not an encoding.
    #[derive(Outgoing, Serialized)]
    #[outgoing(name = "devices/dev42/state")]
    struct DeviceState(Vec<u8>);

    fn state() -> DeviceState {
        DeviceState(b"online".to_vec())
    }

    #[test]
    fn a_call_that_names_nothing_publishes_entirely_under_the_policy() {
        assert_eq!(
            MqttPublishOptions::resolve(None, Qos::AtLeastOnce, false),
            (Qos::AtLeastOnce, false),
            "a reply and a deferred redelivery arrive here with no call site at all"
        );
    }

    #[test]
    fn a_named_argument_wins_and_an_unnamed_one_keeps_the_policy_value() {
        let options = MqttPublishOptions::default().retain(true);
        assert_eq!(
            MqttPublishOptions::resolve(Some(&options), Qos::ExactlyOnce, false),
            (Qos::ExactlyOnce, true),
            "the quality of service the call left alone is still the policy's"
        );
    }

    #[test]
    fn the_steps_compose_in_either_order() {
        let publisher = publisher();
        let state = state();
        let expected = MqttPublishOptions::default()
            .qos(Qos::ExactlyOnce)
            .retain(true);

        let mut forward = publisher.message(&state).qos(Qos::ExactlyOnce).retain(true);
        assert_eq!(*forward.options_mut(), Some(expected));

        let mut reversed = publisher.message(&state).retain(true).qos(Qos::ExactlyOnce);
        assert_eq!(*reversed.options_mut(), Some(expected));
    }

    #[test]
    fn a_builder_no_step_touched_carries_no_options() {
        let publisher = publisher();
        let state = state();
        let mut plain = publisher.message(&state);
        assert_eq!(
            *plain.options_mut(),
            None,
            "nothing to resolve means the policy is the whole answer"
        );
    }

    #[tokio::test]
    async fn a_publish_before_connect_reports_the_missing_connection() {
        let publisher = publisher();
        let msg = OutgoingMessage::new("devices/dev42/state", b"online".as_slice());
        let error = publisher
            .publish(msg, Some(&MqttPublishOptions::default().retain(true)))
            .await
            .expect_err("nothing is connected yet");
        assert!(matches!(error, MqttError::NotConnected));
    }
}
