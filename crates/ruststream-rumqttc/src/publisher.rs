//! [`MqttPublisher`], its [`MqttPublish`] policy, and the per-publish overrides.

use std::fmt;
use std::future::{Future, ready};

use bytes::Bytes;
use rumqttc::v5::mqttbytes::valid_topic;
use ruststream::runtime::{OutSlot, Slot};
use ruststream::{HeaderMap, OutgoingMessage, PairError, PublishPolicy, Publisher};

use crate::broker::{ConnectedMqttBroker, CoreCell};
use crate::error::MqttError;
use crate::filter::Qos;
use crate::message::to_wire_properties;

/// The header the per-message quality of service rides, as the protocol's own numbering
/// (`"0"`, `"1"`, `"2"`).
///
/// [`Publisher::publish`] takes a message and nothing else, so a per-message transport argument
/// reaches the send path as a header - the mechanism the framework names for a delivery option a
/// broker expresses that way. The publisher consumes it: it never travels as a user property.
///
/// Any other value fails the publish with
/// [`MqttError::InvalidPublishArgument`](crate::MqttError::InvalidPublishArgument), naming the
/// header and quoting what arrived. Nothing reaches the wire: a call that asked for a delivery
/// guarantee is not quietly served with the publisher's own.
pub const QOS_HEADER: &str = "mqtt-qos";

/// The header the per-message retain flag rides (`"true"` or `"false"`), consumed - and, on any
/// other value, refused - by the publisher exactly as [`QOS_HEADER`] is.
pub const RETAIN_HEADER: &str = "mqtt-retain";

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
    // Read before anything is built, so a publish naming an argument this crate cannot read is
    // refused rather than sent under the publisher's own guarantee.
    let (per_message, properties) = to_wire_properties(&msg)?;
    let qos = per_message.qos.unwrap_or(qos);
    let retain = per_message.retain.unwrap_or(retain);
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

    async fn publish(&self, msg: OutgoingMessage<'_>) -> Result<(), Self::Error> {
        send(&self.cell, self.qos, self.retain, msg).await
    }
}

/// The two arguments MQTT carries on every PUBLISH packet, reopened at the call site.
///
/// Each method returns an [`MqttPublishOverride`] adapter carrying the value, and the publish
/// continues from there: `publisher.with_retain(true).message(&state).publish()`. An argument
/// the call does not name keeps the publisher's policy value, and the adapter's own methods of
/// the same names refine it further, so the two compose in either order.
///
/// Implemented for the live publisher, the in-process test publisher and the `Out` slot entry, so
/// the same call works in a handler, in a startup hook and under the test harness. Resolving on
/// the entry is what keeps a slot publish attributed to its slot.
///
/// The step yields a plain publisher, so a publish built on it resolves the crate's default codec
/// rather than the include site's; a slot publish that needs the include site's codec goes through
/// the slot's own `message(..)` and names the arguments in its headers ([`QOS_HEADER`],
/// [`RETAIN_HEADER`]).
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
pub trait MqttPublishOptions: Publisher {
    /// Sends with `qos` instead of the publisher's own.
    fn with_qos(&self, qos: Qos) -> MqttPublishOverride<'_, Self> {
        MqttPublishOverride::new(self).with_qos(qos)
    }

    /// Sends retained (or explicitly not retained), whatever the publisher's policy declares.
    ///
    /// A retained message is the last one the broker keeps per topic and hands to each new
    /// subscriber on a matching filter; publishing an empty payload retained clears it.
    fn with_retain(&self, retain: bool) -> MqttPublishOverride<'_, Self> {
        MqttPublishOverride::new(self).with_retain(retain)
    }
}

impl MqttPublishOptions for MqttPublisher {}

// Grafted onto the slot entry a handler body actually holds, next to the framework's own
// capability delegations on it. Resolving the step there keeps the publish attributed to its
// slot; an impl one layer down is reached by autoderef past the entry instead, and a publish
// built on it leaves through the unwrapped publisher, where the harness's per-slot capture never
// sees it.
impl<M: OutSlot, W: MqttPublishOptions, E: Send + Sync, Body> MqttPublishOptions
    for Slot<M, W, E, Body>
{
}

/// A borrowed view of a publisher that sends with per-message `QoS` and retain values instead of
/// the ones its policy fixed, returned by [`MqttPublishOptions`].
///
/// The two arguments ride as the adapter's [base headers](Publisher::base_headers) ([`QOS_HEADER`]
/// and [`RETAIN_HEADER`]), which the publisher consumes on the way to the wire, so they reach the
/// send path without the adapter having to be the publisher itself - which is what lets it wrap a
/// slot entry and keep the publish attributed. A message handed to [`Publisher::publish`] directly
/// has not been through the builder that merges those headers, so the adapter applies them there
/// too, under anything the caller set.
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
#[must_use]
pub struct MqttPublishOverride<'a, P: ?Sized> {
    inner: &'a P,
    base: HeaderMap,
}

impl<'a, P: Publisher + ?Sized> MqttPublishOverride<'a, P> {
    fn new(inner: &'a P) -> Self {
        // Seeded from the wrapped handle so its own base survives the adapter.
        Self {
            inner,
            base: inner.base_headers().cloned().unwrap_or_default(),
        }
    }
}

// Refining an adapter consumes it rather than borrowing it, so the two arguments compose into a
// value that outlives the expression: the borrow it carries is still the publisher's own.
impl<P: ?Sized> MqttPublishOverride<'_, P> {
    /// Also sends with `qos` instead of the publisher's own.
    pub fn with_qos(mut self, qos: Qos) -> Self {
        self.base.insert(QOS_HEADER, qos.as_header());
        self
    }

    /// Also sends retained (or explicitly not retained).
    pub fn with_retain(mut self, retain: bool) -> Self {
        self.base
            .insert(RETAIN_HEADER, if retain { "true" } else { "false" });
        self
    }
}

impl<P: ?Sized> fmt::Debug for MqttPublishOverride<'_, P> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("MqttPublishOverride")
            .field("base_headers", &self.base)
            .finish_non_exhaustive()
    }
}

impl<P: Publisher + ?Sized> Publisher for MqttPublishOverride<'_, P> {
    type Error = P::Error;

    async fn publish(&self, msg: OutgoingMessage<'_>) -> Result<(), Self::Error> {
        let missing: Vec<(&str, &[u8])> = self
            .base
            .iter()
            .filter(|(name, _)| !msg.headers().contains(name))
            .collect();
        if missing.is_empty() {
            // The builder already merged the base in; nothing to add and nothing to copy.
            return self.inner.publish(msg).await;
        }
        let mut headers = msg.headers().clone();
        for (name, value) in missing {
            headers.insert(name, Bytes::copy_from_slice(value));
        }
        self.inner.publish(msg.with_headers(headers)).await
    }

    fn base_headers(&self) -> Option<&HeaderMap> {
        Some(&self.base)
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

    /// What the adapter carries is what its base headers say, since that is the whole of what
    /// reaches the send path.
    fn carried<'a, P: Publisher + ?Sized>(
        adapter: &'a MqttPublishOverride<'_, P>,
    ) -> (Option<&'a str>, Option<&'a str>) {
        let base = adapter.base_headers().expect("the adapter states a base");
        (base.get_str(QOS_HEADER), base.get_str(RETAIN_HEADER))
    }

    #[test]
    fn an_override_starts_from_the_publisher_and_composes_in_either_order() {
        let publisher = publisher();

        let retained = publisher.with_retain(true);
        assert_eq!(carried(&retained), (None, Some("true")));

        let both = retained.with_qos(Qos::ExactlyOnce);
        assert_eq!(
            carried(&both),
            (Some("2"), Some("true")),
            "the earlier argument survives the later one"
        );

        let reversed = publisher.with_qos(Qos::ExactlyOnce).with_retain(true);
        assert_eq!(carried(&reversed), (Some("2"), Some("true")));
    }

    #[test]
    fn an_untouched_argument_is_left_for_the_publisher_policy() {
        let publisher = publisher();
        assert_eq!(
            carried(&publisher.with_qos(Qos::AtMostOnce)),
            (Some("0"), None),
            "an argument the call does not name is not stated at all"
        );
    }

    /// Stands in for the wrapped publisher so a test can read the message the adapter handed on.
    #[derive(Default)]
    struct Recorder(std::sync::Mutex<Option<HeaderMap>>);

    impl Recorder {
        fn seen(&self) -> HeaderMap {
            self.0
                .lock()
                .expect("recorder mutex poisoned")
                .clone()
                .expect("a message reached the publisher")
        }
    }

    impl Publisher for Recorder {
        type Error = MqttError;

        fn publish(&self, msg: OutgoingMessage<'_>) -> impl Future<Output = Result<(), MqttError>> {
            *self.0.lock().expect("recorder mutex poisoned") = Some(msg.headers().clone());
            ready(Ok(()))
        }
    }

    impl MqttPublishOptions for Recorder {}

    #[tokio::test]
    async fn an_override_applies_its_arguments_to_a_message_built_outside_the_builder() {
        let recorder = Recorder::default();
        let mut headers = HeaderMap::new();
        headers.insert("x-tenant", "acme");
        let msg =
            OutgoingMessage::new("devices/dev42/state", b"online".as_slice()).with_headers(headers);

        recorder
            .with_retain(true)
            .with_qos(Qos::ExactlyOnce)
            .publish(msg)
            .await
            .expect("the recorder accepts everything");

        let seen = recorder.seen();
        assert_eq!(seen.get_str(QOS_HEADER), Some("2"));
        assert_eq!(seen.get_str(RETAIN_HEADER), Some("true"));
        assert_eq!(
            seen.get_str("x-tenant"),
            Some("acme"),
            "the caller's own headers survive"
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
