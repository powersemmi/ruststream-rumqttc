//! [`MqttTopic`] and [`MqttFilter`]: the two subscription descriptors.
//!
//! MQTT names two things. A publisher names a *topic*, which carries no wildcards. A subscriber
//! names a *topic filter*, where `+` matches one level and `#` matches the rest. The crate keeps
//! them apart because a retry copy is a publish: a subscription on a topic is reached again by
//! publishing to that topic, and a subscription on a wildcard filter is reached by publishing to
//! none of the topics it matches. [`shared`](MqttTopic::shared) wraps either into an MQTT 5
//! shared subscription (`$share/<group>/<filter>`), which is how competing consumers are
//! expressed at all.

use std::borrow::Cow;
use std::fmt;
use std::future::{Future, ready};

use rumqttc::v5::mqttbytes::{valid_filter, valid_topic};
use ruststream::{
    AddressedCopies, FromName, NamedCopies, RedeliveryAddress, RedeliveryAddressed,
    SubscriptionSource,
};

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

/// Rejects a share group no broker would accept, before any I/O.
fn validate_share_group(group: Option<&String>) -> Result<(), MqttError> {
    if let Some(group) = group
        && (group.is_empty() || group.contains(['/', '+', '#']))
    {
        return Err(MqttError::Invalid(format!(
            "'{group}' is not a valid share group name"
        )));
    }
    Ok(())
}

/// A subscription descriptor for one MQTT topic filter, wildcards included.
///
/// One such subscription reads every topic its filter matches, so it addresses none of them: the
/// mount site names where a deferred retry copy goes. [`MqttTopic`] is the descriptor for a
/// subscription that names a single topic and answers for its own copies.
///
/// ```
/// use ruststream_rumqttc::{MqttFilter, Qos};
///
/// let fleet = MqttFilter::new("devices/+/telemetry")
///     .qos(Qos::AtLeastOnce)
///     .shared("workers");
/// # let _ = fleet;
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
#[must_use]
pub struct MqttFilter {
    filter: String,
    qos: Qos,
    shared: Option<String>,
}

impl MqttFilter {
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

    pub(crate) const fn qos_value(&self) -> Qos {
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
        validate_share_group(self.shared.as_ref())
    }
}

/// A topic filter is all this descriptor needs to exist, so the mount site may supply it: a
/// definition written `#[subscriber(MqttFilter)]` takes its filter from `name(..)` there, and the
/// quality of service and share group stay the defaults.
impl FromName for MqttFilter {
    fn from_name(name: impl Into<Cow<'static, str>>) -> Self {
        Self::new(name.into().into_owned())
    }
}

/// A subscription descriptor for one MQTT topic.
///
/// A topic carries no wildcard, so it is a name a publisher can use: the subscription is reached
/// again by publishing to it, share group or not, because the group takes that copy between its
/// members. That is what lets this descriptor answer for its own retry copies, and what a
/// subscription written with `+` or `#` cannot do - [`MqttFilter`] is the descriptor for those.
///
/// ```
/// use ruststream_rumqttc::{MqttTopic, Qos};
///
/// let source = MqttTopic::new("devices/dev42/telemetry")
///     .qos(Qos::AtLeastOnce)
///     .shared("workers");
/// # let _ = source;
/// ```
#[derive(Clone, PartialEq, Eq)]
#[must_use]
pub struct MqttTopic(MqttFilter);

impl fmt::Debug for MqttTopic {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("MqttTopic")
            .field("topic", &self.0.filter)
            .field("qos", &self.0.qos)
            .field("shared", &self.0.shared)
            .finish()
    }
}

impl MqttTopic {
    /// Names the topic. A value carrying a wildcard is refused when the subscription opens,
    /// naming [`MqttFilter`] as the descriptor that takes one.
    pub fn new(topic: impl Into<String>) -> Self {
        Self(MqttFilter::new(topic))
    }

    /// Sets the delivery quality of service. Defaults to [`Qos::AtLeastOnce`].
    pub fn qos(mut self, qos: Qos) -> Self {
        self.0 = self.0.qos(qos);
        self
    }

    /// Makes this an MQTT 5 shared subscription in `group`: the broker distributes matching
    /// messages across the group's consumers instead of fanning out to each.
    pub fn shared(mut self, group: impl Into<String>) -> Self {
        self.0 = self.0.shared(group);
        self
    }

    /// The topic this subscribes to (without any share group).
    #[must_use]
    pub fn topic(&self) -> &str {
        self.0.filter()
    }

    /// The descriptor as a filter, which is the form a subscription opens with on the wire.
    pub(crate) fn into_filter(self) -> MqttFilter {
        self.0
    }

    /// Rejects a value no publisher could name, before any I/O. A topic is a string the service
    /// may read from its configuration, so which of the two descriptors fits is the author's
    /// declaration and this is what holds them to it.
    pub(crate) fn validate(&self) -> Result<(), MqttError> {
        // The client reads an empty string as a valid topic; a broker does not.
        if self.0.filter.is_empty() {
            return Err(MqttError::Invalid(
                "an MQTT topic must not be empty".to_owned(),
            ));
        }
        if !valid_topic(&self.0.filter) {
            return Err(MqttError::Invalid(format!(
                "'{}' is not an MQTT topic: wildcards are subscribe-only, so a filter carrying \
                 '+' or '#' subscribes with MqttFilter instead",
                self.0.filter
            )));
        }
        validate_share_group(self.0.shared.as_ref())
    }

    /// The topic a publisher reaches this subscription on.
    fn address(&self) -> Result<RedeliveryAddress, MqttError> {
        // The copy path promises that the reported address reaches this subscription, so a value
        // that is not a topic is refused here rather than published to.
        self.validate()?;
        Ok(RedeliveryAddress::new(self.0.filter.clone()))
    }
}

/// A topic is all this descriptor needs to exist, so the mount site may supply it: a definition
/// written `#[subscriber(MqttTopic)]` takes its topic from `name(..)` there, and the quality of
/// service and share group stay the defaults.
impl FromName for MqttTopic {
    fn from_name(name: impl Into<Cow<'static, str>>) -> Self {
        Self::new(name.into().into_owned())
    }
}

impl SubscriptionSource<ConnectedMqttBroker> for MqttTopic {
    type Subscriber = MqttSubscriber;
    /// One topic is one destination, so the descriptor says where a copy of a delivery reaches
    /// this subscription again and the mount site names nothing.
    type Copies = AddressedCopies;

    fn name(&self) -> &str {
        self.topic()
    }

    async fn subscribe(self, connected: &ConnectedMqttBroker) -> Result<MqttSubscriber, MqttError> {
        connected.subscribe_topic(self).await
    }
}

/// The plain topic, never the share group's wire form: `$share/<group>/<topic>` is a
/// subscribe-side name, and a publish to it would reach a topic of that literal spelling.
impl RedeliveryAddressed<ConnectedMqttBroker> for MqttTopic {
    fn redelivery_address(
        &self,
        _connected: &ConnectedMqttBroker,
    ) -> impl Future<Output = Result<RedeliveryAddress, MqttError>> + Send {
        ready(self.address())
    }
}

impl SubscriptionSource<ConnectedMqttBroker> for MqttFilter {
    type Subscriber = MqttSubscriber;
    /// A filter matches many topics and is a topic itself only by accident, so the descriptor
    /// addresses no copy and the mount site names where one goes.
    type Copies = NamedCopies;

    fn name(&self) -> &str {
        self.filter()
    }

    async fn subscribe(self, connected: &ConnectedMqttBroker) -> Result<MqttSubscriber, MqttError> {
        connected.subscribe_filter(self).await
    }
}

/// The same descriptors open the subscription on the in-process broker, so what a service
/// declares for production is what the harness mounts: no second descriptor type, and no name
/// rewritten at the mount site.
///
/// Every part of the descriptor that decides what a handler sees is honoured there: the filter
/// selects the same topics, [`shared`](MqttFilter::shared) makes the subscription a competing
/// consumer rather than another copy, and [`qos`](MqttFilter::qos) decides whether a delivery can
/// be settled at all - [`Qos::AtMostOnce`] reports
/// [`AckError::Unsupported`](ruststream::AckError::Unsupported) in process exactly as it does on
/// the wire. What is left is the protocol itself: the handshake behind an acknowledged `QoS`, the
/// retained message, the session that redelivers. Those need a server, and the live suite is where
/// they are checked.
#[cfg(feature = "testing")]
impl SubscriptionSource<ConnectedMqttTestBroker> for MqttTopic {
    type Subscriber = MqttTestSubscriber;
    type Copies = AddressedCopies;

    fn name(&self) -> &str {
        self.topic()
    }

    async fn subscribe(
        self,
        connected: &ConnectedMqttTestBroker,
    ) -> Result<MqttTestSubscriber, MqttError> {
        connected.subscribe_topic(self).await
    }
}

/// The same answer as against a server, so a registration starts on both brokers or on neither.
#[cfg(feature = "testing")]
impl RedeliveryAddressed<ConnectedMqttTestBroker> for MqttTopic {
    fn redelivery_address(
        &self,
        _connected: &ConnectedMqttTestBroker,
    ) -> impl Future<Output = Result<RedeliveryAddress, MqttError>> + Send {
        ready(self.address())
    }
}

#[cfg(feature = "testing")]
impl SubscriptionSource<ConnectedMqttTestBroker> for MqttFilter {
    type Subscriber = MqttTestSubscriber;
    type Copies = NamedCopies;

    fn name(&self) -> &str {
        self.filter()
    }

    async fn subscribe(
        self,
        connected: &ConnectedMqttTestBroker,
    ) -> Result<MqttTestSubscriber, MqttError> {
        connected.subscribe_filter(self).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn invalid_filters_are_rejected_before_io() {
        assert!(MqttFilter::new("a/#/b").validate().is_err());
        assert!(MqttFilter::new("").validate().is_err());
    }

    #[test]
    fn invalid_share_groups_are_rejected_before_io() {
        assert!(MqttFilter::new("a").shared("g/1").validate().is_err());
        assert!(MqttFilter::new("a").shared("").validate().is_err());
        assert!(MqttTopic::new("a").shared("g/1").validate().is_err());
    }

    #[test]
    fn shared_filters_wrap_on_the_wire_only() {
        let filter = MqttFilter::new("orders/+").shared("workers");
        assert_eq!(filter.filter(), "orders/+");
        assert_eq!(filter.wire_filter(), "$share/workers/orders/+");
    }

    /// A deferred retry is published under the reported address, so the address has to be a topic
    /// a publisher can name. The descriptor that answers one takes only topics, and says which
    /// descriptor takes a filter when it meets one.
    #[test]
    fn a_topic_is_the_address_a_retry_is_published_to() {
        assert_eq!(
            MqttTopic::new("devices/dev42/telemetry")
                .address()
                .expect("a topic is its own address"),
            RedeliveryAddress::new("devices/dev42/telemetry")
        );
        for wildcard in ["devices/+/telemetry", "devices/#"] {
            let refused = MqttTopic::new(wildcard)
                .address()
                .expect_err("a filter is not an address");
            assert!(
                refused.to_string().contains("MqttFilter"),
                "the refusal names the descriptor that takes a filter: {refused}"
            );
        }
        assert!(MqttTopic::new("").address().is_err());
    }

    /// A share group is subscribe-side spelling. The address is the plain topic, where a publish
    /// reaches the group and one member takes it.
    #[test]
    fn a_shared_subscription_is_reached_through_its_plain_topic() {
        assert_eq!(
            MqttTopic::new("jobs")
                .shared("workers")
                .address()
                .expect("a topic is its own address"),
            RedeliveryAddress::new("jobs")
        );
    }

    /// A name the mount site supplies builds the same descriptor a service writes inline, so
    /// `#[subscriber(MqttFilter)]` and `MqttFilter::new(..)` reach one subscription.
    #[test]
    fn a_descriptor_built_from_a_name_alone_is_the_plain_one() {
        assert_eq!(
            MqttFilter::from_name("devices/+/telemetry"),
            MqttFilter::new("devices/+/telemetry")
        );
        assert_eq!(
            MqttTopic::from_name("devices/dev42/telemetry"),
            MqttTopic::new("devices/dev42/telemetry")
        );
    }
}
