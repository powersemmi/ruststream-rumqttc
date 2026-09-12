//! [`MqttPublisher`], its [`MqttPublish`] policy, and the per-message settings.

use std::fmt;
use std::future::{Future, ready};

use bytes::Bytes;
use rumqttc::v5::mqttbytes::valid_topic;
use ruststream::runtime::{PublishBuilder, PublishSink};
use ruststream::{OutgoingMessage, PairError, PublishPolicy, Publisher};

use crate::broker::{ConnectedMqttBroker, CoreCell};
use crate::error::MqttError;
use crate::filter::Qos;
use crate::message::to_wire_properties;
#[cfg(feature = "testing")]
use crate::testing::{ConnectedMqttTestBroker, MqttTestPublisher};

/// The single send path: every publishing form resolves to a `QoS` and a retain flag, and the
/// wire work happens here once.
async fn send(
    cell: &CoreCell,
    qos: Qos,
    retain: bool,
    msg: OutgoingMessage<'_>,
) -> Result<(), MqttError> {
    let core = cell.get().ok_or(MqttError::NotConnected)?;
    core.shared.ensure_open()?;
    // The client's send-path error cannot say why a request failed, so the topic is
    // validated here; a remaining failure unambiguously means the connection is gone.
    if !valid_topic(msg.name()) {
        return Err(MqttError::Publish {
            topic: msg.name().to_owned(),
            reason: "not a valid MQTT topic (wildcards are subscribe-only)".to_owned(),
        });
    }
    let properties = to_wire_properties(&msg);
    let payload = Bytes::copy_from_slice(msg.payload());
    let outcome = match properties {
        Some(properties) => {
            core.client
                .publish_bytes_with_properties(
                    msg.name(),
                    qos.to_client(),
                    retain,
                    payload,
                    properties,
                )
                .await
        }
        None => {
            core.client
                .publish_bytes(msg.name(), qos.to_client(), retain, payload)
                .await
        }
    };
    outcome.map_err(|_| MqttError::Publish {
        topic: msg.name().to_owned(),
        reason: "the mqtt connection task has shut down".to_owned(),
    })
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
    type Error = MqttError;
    type Options = MqttPublishOptions;

    async fn publish(
        &self,
        msg: OutgoingMessage<'_>,
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

    /// The quality of service this policy publishes at.
    #[cfg(feature = "testing")]
    pub(crate) const fn qos_value(self) -> Qos {
        self.qos
    }

    /// Whether this policy publishes retained.
    #[cfg(feature = "testing")]
    pub(crate) const fn retain_value(self) -> bool {
        self.retain
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
}

/// The same policy pairs against the in-process broker, so a routes file mounts on both brokers as
/// written - the destination, the codec and the slot it is attached to are the mount site's, and
/// none of them changes with the transport underneath.
///
/// The quality of service survives the pairing, because it is what says whether a subscriber can
/// settle the delivery at all: publish at [`Qos::AtMostOnce`] in process and the handler meets the
/// same [`AckError::Unsupported`](ruststream::AckError::Unsupported) a server would have produced.
/// The retain flag stops here - nothing in process keeps a last message per topic - and neither
/// argument is recorded as a header, because on the wire the publisher consumes them, so a
/// delivery here carries exactly what a subscriber would see. A test on this transport therefore
/// says what was published, where, and whether it could be acknowledged; that it was retained, or
/// that the acknowledgement completed a protocol handshake, is the live suite's to check.
#[cfg(feature = "testing")]
impl PublishPolicy<ConnectedMqttTestBroker> for MqttPublish {
    type Live = MqttTestPublisher;

    fn pair(
        self,
        connected: &ConnectedMqttTestBroker,
    ) -> impl Future<Output = Result<Self::Live, PairError>> {
        ready(Ok(connected.publisher_with(self)))
    }
}

#[cfg(test)]
mod tests {
    use ruststream::runtime::PublishExt;
    use ruststream::{Outgoing, Serialized};

    use super::*;
    use crate::broker::MqttBroker;

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
