//! [`MqttTopic`]: the subscription descriptor.
//!
//! Wildcards are the protocol's own (`+` per level, `#` terminal); `shared` wraps the filter
//! into an MQTT 5 shared subscription (`$share/<group>/<filter>`), which is how competing
//! consumers are expressed at all.

use std::borrow::Cow;
use std::future::{Future, ready};

use rumqttc::v5::mqttbytes::{valid_filter, valid_topic};
use ruststream::{FromName, RedeliveryAddress, SubscriptionSource};

use crate::broker::ConnectedMqttBroker;
use crate::error::MqttError;
use crate::subscriber::MqttSubscriber;
#[cfg(feature = "testing")]
use crate::testing::{ConnectedMqttTestBroker, MqttTestSubscriber};

/// Delivery quality of service for a subscription or a publish policy.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum Qos {
    /// Fire and forget; deliveries carry no acknowledgement
    /// ([`AckError::Unsupported`](ruststream::AckError::Unsupported)).
    AtMostOnce,
    /// Acknowledged delivery. The default.
    #[default]
    AtLeastOnce,
    /// Exactly-once handshake (the client completes the second leg automatically).
    ExactlyOnce,
}

impl Qos {
    pub(crate) fn to_client(self) -> rumqttc::v5::mqttbytes::QoS {
        match self {
            Self::AtMostOnce => rumqttc::v5::mqttbytes::QoS::AtMostOnce,
            Self::AtLeastOnce => rumqttc::v5::mqttbytes::QoS::AtLeastOnce,
            Self::ExactlyOnce => rumqttc::v5::mqttbytes::QoS::ExactlyOnce,
        }
    }
}

/// A subscription descriptor for one MQTT topic filter.
///
/// Implements [`SubscriptionSource`], so it can sit inline in the `#[subscriber(..)]`
/// decorator - for the real broker and, under the `testing` feature, for the in-process one, so
/// the declaration a service ships is the declaration its tests mount:
///
/// ```
/// use ruststream_rumqttc::{MqttTopic, Qos};
///
/// let source = MqttTopic::new("devices/+/telemetry")
///     .qos(Qos::AtLeastOnce)
///     .shared("workers");
/// # let _ = source;
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
#[must_use]
pub struct MqttTopic {
    filter: String,
    qos: Qos,
    shared: Option<String>,
}

impl MqttTopic {
    /// Names the topic filter, with wildcards as the protocol defines them.
    pub fn new(filter: impl Into<String>) -> Self {
        Self {
            filter: filter.into(),
            qos: Qos::default(),
            shared: None,
        }
    }

    /// Sets the delivery quality of service. Defaults to [`Qos::AtLeastOnce`].
    pub fn qos(mut self, qos: Qos) -> Self {
        self.qos = qos;
        self
    }

    /// Makes this an MQTT 5 shared subscription in `group`: the broker distributes matching
    /// messages across the group's consumers instead of fanning out to each.
    pub fn shared(mut self, group: impl Into<String>) -> Self {
        self.shared = Some(group.into());
        self
    }

    /// The plain topic filter (without any share group).
    #[must_use]
    pub fn filter(&self) -> &str {
        &self.filter
    }

    pub(crate) fn qos_value(&self) -> Qos {
        self.qos
    }

    /// Consumes the descriptor into what a subscription registry needs of it: the filter to match
    /// on, the wire filter that names its share group, and the quality of service its deliveries
    /// are settled under.
    #[cfg(feature = "testing")]
    pub(crate) fn into_parts(self) -> (String, Option<String>, Qos) {
        let group = self.shared.as_ref().map(|_| self.wire_filter());
        (self.filter, group, self.qos)
    }

    /// The filter as subscribed on the wire (`$share/<group>/<filter>` when shared).
    pub(crate) fn wire_filter(&self) -> String {
        self.shared.as_ref().map_or_else(
            || self.filter.clone(),
            |group| format!("$share/{group}/{}", self.filter),
        )
    }

    /// Rejects descriptors that cannot form a subscription, before any I/O. The client's own
    /// send-path error cannot say why a request failed, so validation happens here.
    pub(crate) fn validate(&self) -> Result<(), MqttError> {
        if !valid_filter(&self.filter) {
            return Err(MqttError::Invalid(format!(
                "'{}' is not a valid MQTT topic filter",
                self.filter
            )));
        }
        if let Some(group) = &self.shared
            && (group.is_empty() || group.contains(['/', '+', '#']))
        {
            return Err(MqttError::Invalid(format!(
                "'{group}' is not a valid share group name"
            )));
        }
        Ok(())
    }
}

/// A topic filter is all this descriptor needs to exist, so the mount site may supply it: a
/// definition written `#[subscriber(MqttTopic)]` takes its filter from `name(..)` there, and the
/// quality of service and share group stay the defaults.
impl FromName for MqttTopic {
    fn from_name(name: impl Into<Cow<'static, str>>) -> Self {
        Self::new(name.into().into_owned())
    }
}

/// The topic a publisher reaches the subscription on `filter` with, when there is one.
///
/// A concrete filter is a topic, so publishing to it reaches the subscription - including a shared
/// one, where the group takes the copy between its members, which is what a redelivered message
/// should meet. A wildcard filter is not a topic at all: `+` and `#` are subscribe-only, and a
/// publish naming one is refused rather than delivered anywhere. The broker says so instead of
/// naming an address that reaches nothing.
pub(crate) fn redelivery_topic(filter: &str) -> Option<RedeliveryAddress> {
    (!filter.is_empty() && valid_topic(filter)).then(|| RedeliveryAddress::new(filter.to_owned()))
}

impl SubscriptionSource<ConnectedMqttBroker> for MqttTopic {
    type Subscriber = MqttSubscriber;

    fn name(&self) -> &str {
        self.filter()
    }

    async fn subscribe(self, connected: &ConnectedMqttBroker) -> Result<MqttSubscriber, MqttError> {
        connected.subscribe_topic(self).await
    }

    /// The plain filter, never the share group's wire form: `$share/<group>/<filter>` is a
    /// subscribe-side name, and a publish to it would reach a topic of that literal spelling.
    fn redelivery_address(
        &self,
        connected: &ConnectedMqttBroker,
    ) -> impl Future<Output = Result<Option<RedeliveryAddress>, MqttError>> + Send {
        let _ = connected;
        ready(Ok(redelivery_topic(self.filter())))
    }
}

/// The same descriptor opens the subscription on the in-process broker, so what a service
/// declares for production is what the harness mounts: no second descriptor type, and no name
/// rewritten at the mount site.
///
/// Every part of the descriptor that decides what a handler sees is honoured there: the filter
/// selects the same topics, [`shared`](MqttTopic::shared) makes the subscription a competing
/// consumer rather than another copy, and [`qos`](MqttTopic::qos) decides whether a delivery can
/// be settled at all - [`Qos::AtMostOnce`] reports
/// [`AckError::Unsupported`](ruststream::AckError::Unsupported) in process exactly as it does on
/// the wire. What is left is the protocol itself: the handshake behind an acknowledged `QoS`, the
/// retained message, the session that redelivers. Those need a server, and the live suite is where
/// they are checked.
#[cfg(feature = "testing")]
impl SubscriptionSource<ConnectedMqttTestBroker> for MqttTopic {
    type Subscriber = MqttTestSubscriber;

    fn name(&self) -> &str {
        self.filter()
    }

    async fn subscribe(
        self,
        connected: &ConnectedMqttTestBroker,
    ) -> Result<MqttTestSubscriber, MqttError> {
        connected.subscribe_topic(self).await
    }

    /// The same answer as against a server, so a scope wired with `retry_via` either starts on
    /// both brokers or on neither.
    fn redelivery_address(
        &self,
        connected: &ConnectedMqttTestBroker,
    ) -> impl Future<Output = Result<Option<RedeliveryAddress>, MqttError>> + Send {
        let _ = connected;
        ready(Ok(redelivery_topic(self.filter())))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn invalid_filters_are_rejected_before_io() {
        assert!(MqttTopic::new("a/#/b").validate().is_err());
        assert!(MqttTopic::new("").validate().is_err());
    }

    #[test]
    fn invalid_share_groups_are_rejected_before_io() {
        assert!(MqttTopic::new("a").shared("g/1").validate().is_err());
        assert!(MqttTopic::new("a").shared("").validate().is_err());
    }

    #[test]
    fn shared_filters_wrap_on_the_wire_only() {
        let topic = MqttTopic::new("orders/+").shared("workers");
        assert_eq!(topic.filter(), "orders/+");
        assert_eq!(topic.wire_filter(), "$share/workers/orders/+");
    }

    /// A deferred retry is published under the reported address, so the address has to be a topic
    /// a publisher can name. A concrete filter is one; a wildcard is not, and saying so costs the
    /// fallback rather than losing the message to a publish that reaches nothing.
    #[test]
    fn a_concrete_filter_is_the_topic_a_retry_is_published_to() {
        assert_eq!(
            redelivery_topic("devices/dev42/telemetry"),
            Some(RedeliveryAddress::new("devices/dev42/telemetry"))
        );
        assert_eq!(redelivery_topic("devices/+/telemetry"), None);
        assert_eq!(redelivery_topic("devices/#"), None);
        assert_eq!(redelivery_topic(""), None);
    }

    /// A share group is subscribe-side spelling. The address is the plain filter, where a publish
    /// reaches the group and one member takes it.
    #[test]
    fn a_shared_subscription_is_reached_through_its_plain_filter() {
        assert_eq!(
            redelivery_topic(MqttTopic::new("jobs").shared("workers").filter()),
            Some(RedeliveryAddress::new("jobs"))
        );
    }

    /// A filter the mount site names builds the same descriptor a service writes inline, so
    /// `#[subscriber(MqttTopic)]` and `MqttTopic::new(..)` reach one subscription.
    #[test]
    fn a_descriptor_built_from_a_name_alone_is_the_plain_one() {
        assert_eq!(
            MqttTopic::from_name("devices/+/telemetry"),
            MqttTopic::new("devices/+/telemetry")
        );
    }
}
