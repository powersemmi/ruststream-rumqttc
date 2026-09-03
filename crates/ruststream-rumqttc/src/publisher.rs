//! [`MqttPublisher`], its [`MqttPublish`] policy, and the per-publish overrides.

use std::future::{Future, ready};

use bytes::Bytes;
use rumqttc::v5::mqttbytes::valid_topic;
use ruststream::runtime::{OutSlot, SlotPublisher};
use ruststream::{OutgoingMessage, PairError, PublishPolicy, Publisher};

use crate::broker::{ConnectedMqttBroker, CoreCell};
use crate::error::MqttError;
use crate::filter::Qos;
use crate::message::to_publish_properties;

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
    let payload = Bytes::copy_from_slice(msg.payload());
    let outcome = match to_publish_properties(&msg) {
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

impl std::fmt::Debug for MqttPublisher {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
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

    async fn publish(&self, msg: OutgoingMessage<'_>) -> Result<(), Self::Error> {
        send(&self.cell, self.qos, self.retain, msg).await
    }
}

/// A borrowed view of an [`MqttPublisher`] that sends with per-message `QoS` and retain values
/// instead of the ones its policy fixed.
///
/// Produced by [`MqttPublishOptions`], and a [`Publisher`] itself, so the framework's publish
/// builder continues from it unchanged.
///
/// # Examples
///
/// ```
/// use ruststream::runtime::PublishExt;
/// use ruststream::{Outgoing, Serialized};
/// use ruststream_rumqttc::{MqttBroker, MqttPublishOptions, Qos};
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
///     .with_retain(true)
///     .with_qos(Qos::ExactlyOnce)
///     .message(&DeviceState(b"online".to_vec()))
///     .publish()
///     .await?;
/// # Ok(())
/// # }
/// ```
#[derive(Debug, Clone, Copy)]
#[must_use]
pub struct MqttPublishOverride<'a> {
    publisher: &'a MqttPublisher,
    qos: Qos,
    retain: bool,
}

// Refining an adapter consumes it rather than borrowing it, so the two arguments compose into a
// value that outlives the expression: the borrow it carries is still the publisher's own.
impl MqttPublishOverride<'_> {
    /// Also sends with `qos` instead of the publisher's own.
    pub const fn with_qos(self, qos: Qos) -> Self {
        Self { qos, ..self }
    }

    /// Also sends retained (or explicitly not retained).
    pub const fn with_retain(self, retain: bool) -> Self {
        Self { retain, ..self }
    }
}

impl Publisher for MqttPublishOverride<'_> {
    type Error = MqttError;

    async fn publish(&self, msg: OutgoingMessage<'_>) -> Result<(), Self::Error> {
        send(&self.publisher.cell, self.qos, self.retain, msg).await
    }
}

/// The two arguments MQTT carries on every PUBLISH packet, reopened at the call site.
///
/// Each method returns an [`MqttPublishOverride`] adapter carrying the value, and the publish
/// continues from there: `publisher.with_retain(true).message(&state).publish()`.
///
/// Implemented for the publisher and for an `Out` slot holding one; the adapter's own methods of
/// the same names refine it further, so the two arguments compose in either order. A publish made
/// from a slot through the adapter reaches the broker's publish log, but the `TestApp` harness
/// does not attribute it to the slot.
///
/// # Examples
///
/// ```
/// use ruststream::Publisher;
/// use ruststream::OutgoingMessage;
/// use ruststream_rumqttc::{MqttBroker, MqttPublishOptions};
///
/// # async fn demo() -> Result<(), Box<dyn std::error::Error>> {
/// let publisher = MqttBroker::new("mqtt://localhost:1883", "states").publisher();
/// let msg = OutgoingMessage::new("devices/dev42/state", b"online".as_slice());
/// publisher.with_retain(true).publish(msg).await?;
/// # Ok(())
/// # }
/// ```
pub trait MqttPublishOptions {
    /// Sends with `qos` instead of the publisher's own.
    fn with_qos(&self, qos: Qos) -> MqttPublishOverride<'_>;

    /// Sends retained (or explicitly not retained), whatever the publisher's policy declares.
    ///
    /// A retained message is the last one the broker keeps per topic and hands to each new
    /// subscriber on a matching filter; publishing an empty payload retained clears it.
    fn with_retain(&self, retain: bool) -> MqttPublishOverride<'_>;
}

impl MqttPublishOptions for MqttPublisher {
    fn with_qos(&self, qos: Qos) -> MqttPublishOverride<'_> {
        MqttPublishOverride {
            publisher: self,
            qos,
            retain: self.retain,
        }
    }

    fn with_retain(&self, retain: bool) -> MqttPublishOverride<'_> {
        MqttPublishOverride {
            publisher: self,
            qos: self.qos,
            retain,
        }
    }
}

// The slot wrapper delegates the framework's own capability vocabulary; a broker-defined one is
// grafted on once, for every marker, and reaches the paired publisher through `inner`.
impl<P: MqttPublishOptions, M: OutSlot> MqttPublishOptions for SlotPublisher<P, M> {
    fn with_qos(&self, qos: Qos) -> MqttPublishOverride<'_> {
        self.inner().with_qos(qos)
    }

    fn with_retain(&self, retain: bool) -> MqttPublishOverride<'_> {
        self.inner().with_retain(retain)
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
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::broker::MqttBroker;

    fn publisher() -> MqttPublisher {
        MqttBroker::new("mqtt://localhost:1883", "overrides").publisher()
    }

    #[test]
    fn an_override_starts_from_the_publisher_and_composes_in_either_order() {
        let publisher = publisher();

        let retained = publisher.with_retain(true);
        assert!(retained.retain);
        assert_eq!(retained.qos, Qos::AtLeastOnce, "the publisher's own QoS");

        let both = retained.with_qos(Qos::ExactlyOnce);
        assert!(both.retain, "the earlier argument survives the later one");
        assert_eq!(both.qos, Qos::ExactlyOnce);

        let reversed = publisher.with_qos(Qos::ExactlyOnce).with_retain(true);
        assert!(reversed.retain);
        assert_eq!(reversed.qos, Qos::ExactlyOnce);
    }

    #[test]
    fn an_untouched_argument_keeps_the_publisher_policy() {
        let publisher = publisher();
        assert!(
            !publisher.with_qos(Qos::AtMostOnce).retain,
            "the default policy does not retain"
        );
    }

    #[tokio::test]
    async fn an_override_reports_the_missing_connection_like_the_publisher() {
        let publisher = publisher();
        let msg = OutgoingMessage::new("devices/dev42/state", b"online".as_slice());
        let error = publisher
            .with_retain(true)
            .publish(msg)
            .await
            .expect_err("nothing is connected yet");
        assert!(matches!(error, MqttError::NotConnected));
    }
}
