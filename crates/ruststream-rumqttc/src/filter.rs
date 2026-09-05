//! [`MqttTopic`]: the subscription descriptor.
//!
//! Wildcards are the protocol's own (`+` per level, `#` terminal); `shared` wraps the filter
//! into an MQTT 5 shared subscription (`$share/<group>/<filter>`), which is how competing
//! consumers are expressed at all.

use rumqttc::v5::mqttbytes::valid_filter;
use ruststream::SubscriptionSource;

use crate::broker::ConnectedMqttBroker;
use crate::error::MqttError;
use crate::subscriber::MqttSubscriber;

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

    /// The protocol's own numbering, which is what the value travels as on
    /// [`QOS_HEADER`](crate::QOS_HEADER).
    pub(crate) const fn as_header(self) -> &'static str {
        match self {
            Self::AtMostOnce => "0",
            Self::AtLeastOnce => "1",
            Self::ExactlyOnce => "2",
        }
    }

    pub(crate) fn from_header(value: &[u8]) -> Option<Self> {
        match value {
            b"0" => Some(Self::AtMostOnce),
            b"1" => Some(Self::AtLeastOnce),
            b"2" => Some(Self::ExactlyOnce),
            _ => None,
        }
    }
}

/// A subscription descriptor for one MQTT topic filter.
///
/// Implements [`SubscriptionSource`], so it can sit inline in the `#[subscriber(..)]`
/// decorator:
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

impl SubscriptionSource<ConnectedMqttBroker> for MqttTopic {
    type Subscriber = MqttSubscriber;

    fn name(&self) -> &str {
        self.filter()
    }

    async fn subscribe(self, connected: &ConnectedMqttBroker) -> Result<MqttSubscriber, MqttError> {
        connected.subscribe_topic(self).await
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
}
