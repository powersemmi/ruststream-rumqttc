//! [`MqttContext`]: what a delivery carries beyond its payload and headers.
//!
//! MQTT publishes to a topic and subscribes with a filter, so the two are not the same string
//! whenever a filter carries a wildcard. The subscription name a handler reads with `ctx.name()`
//! is the filter; the topic the message was actually published to is here, under the
//! [`DeliveryTopic`] key.

use ruststream::{BuildContext, ContextField, Field};

use crate::message::MqttMessage;

/// The per-delivery context of this broker.
///
/// Read a field with a key rather than by field access: `ctx.context(DeliveryTopic)` in a handler
/// body, `cx.context(DeliveryTopic)` in a publish transform on the reply or the retry position.
///
/// # Examples
///
/// ```
/// use ruststream_rumqttc::{DeliveryTopic, MqttContext};
/// use ruststream::Field;
///
/// let cx = MqttContext::new("devices/dev42/telemetry");
/// assert_eq!(DeliveryTopic.get(&cx), "devices/dev42/telemetry");
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct MqttContext {
    topic: String,
}

impl MqttContext {
    /// Builds the context of a delivery that arrived on `topic`.
    #[must_use]
    pub fn new(topic: impl Into<String>) -> Self {
        Self {
            topic: topic.into(),
        }
    }

    /// The concrete topic this delivery was published to, never the filter that matched it.
    #[must_use]
    pub fn topic(&self) -> &str {
        &self.topic
    }
}

/// The key for [`MqttContext::topic`].
///
/// It reads the topic borrowed through `ctx.context(DeliveryTopic)`, and owned through the
/// `Ctx<DeliveryTopic>` extractor, which is also what fixes a handler's context type to
/// [`MqttContext`] without a `Context` parameter.
///
/// # Examples
///
/// ```
/// use ruststream::ContextField;
/// use ruststream_rumqttc::{DeliveryTopic, MqttContext};
///
/// let cx = MqttContext::new("devices/dev42/telemetry");
/// assert_eq!(DeliveryTopic.read(&cx), "devices/dev42/telemetry");
/// ```
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct DeliveryTopic;

impl Field<MqttContext> for DeliveryTopic {
    type Value<'a> = &'a str;

    fn get(self, src: &MqttContext) -> &str {
        src.topic()
    }
}

impl ContextField for DeliveryTopic {
    type Context = MqttContext;
    type Value = String;

    fn read(self, src: &MqttContext) -> String {
        src.topic.clone()
    }
}

impl BuildContext<MqttMessage> for MqttContext {
    fn build(msg: &MqttMessage) -> Self {
        Self::new(msg.topic())
    }
}
