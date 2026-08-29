//! [`MqttMessage`] and the mapping between `RustStream` headers and MQTT 5 properties.
//!
//! User properties carry headers natively; the well-known `content-type`, `reply-to`, and
//! `correlation-id` headers ride the matching first-class MQTT 5 properties, so no envelope
//! format is invented and non-Rust peers see plain MQTT messages.

use bytes::Bytes;
use rumqttc::v5::AsyncClient;
use rumqttc::v5::mqttbytes::QoS;
use rumqttc::v5::mqttbytes::v5::{Publish, PublishProperties};
use ruststream::{AckError, HeaderMap, IncomingMessage, OutgoingMessage};

/// A message delivered by an [`MqttSubscriber`](crate::MqttSubscriber).
///
/// `ack` acknowledges through the protocol for `QoS` 1 (`PUBACK`) and `QoS` 2 (`PUBREC`, with the
/// client completing the handshake); `QoS` 0 deliveries report
/// [`AckError::Unsupported`]. MQTT has no negative acknowledgement, so `nack(requeue = true)`
/// reports [`AckError::Unsupported`] as well - unacknowledged messages redeliver when the
/// session resumes - and `nack(requeue = false)` acknowledges (dropping is the only terminal
/// outcome the protocol offers).
pub struct MqttMessage {
    payload: Bytes,
    headers: HeaderMap,
    topic: String,
    /// `None` when this delivery carries no acknowledgement: `QoS` 0, or a fanned-out copy on
    /// an overlapping filter (the wire ack belongs to exactly one delivery).
    acker: Option<(AsyncClient, Publish)>,
}

impl std::fmt::Debug for MqttMessage {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MqttMessage")
            .field("topic", &self.topic)
            .field("payload_len", &self.payload.len())
            .finish_non_exhaustive()
    }
}

impl MqttMessage {
    pub(crate) fn new(topic: String, publish: &Publish, client: Option<AsyncClient>) -> Self {
        let mut headers = HeaderMap::new();
        if let Some(properties) = &publish.properties {
            for (name, value) in &properties.user_properties {
                headers.insert(name.clone(), value.clone());
            }
            if let Some(content_type) = &properties.content_type {
                headers.insert("content-type", content_type.clone());
            }
            if let Some(response_topic) = &properties.response_topic {
                headers.insert("reply-to", response_topic.clone());
            }
            if let Some(correlation) = &properties.correlation_data {
                headers.insert("correlation-id", correlation.clone());
            }
        }
        let acker = match publish.qos {
            QoS::AtMostOnce => None,
            _ => client.map(|client| (client, publish.clone())),
        };
        Self {
            payload: publish.payload.clone(),
            headers,
            topic,
            acker,
        }
    }

    /// The topic this message was published to (the real topic, never a `$share` filter).
    #[must_use]
    pub fn topic(&self) -> &str {
        &self.topic
    }
}

impl IncomingMessage for MqttMessage {
    fn payload(&self) -> &[u8] {
        &self.payload
    }

    fn headers(&self) -> &HeaderMap {
        &self.headers
    }

    async fn ack(self) -> Result<(), AckError> {
        let Some((client, publish)) = self.acker else {
            return Err(AckError::Unsupported);
        };
        client
            .ack(&publish)
            .await
            .map_err(|_| AckError::Broker(Box::from("the mqtt connection task has shut down")))
    }

    async fn nack(self, requeue: bool) -> Result<(), AckError> {
        if requeue {
            // MQTT has no negative acknowledgement: an unacked message redelivers only when
            // the session resumes. Reporting Unsupported is honest; pretending would ack.
            Err(AckError::Unsupported)
        } else {
            self.ack().await
        }
    }
}

/// Builds the wire properties for an outgoing publish. Returns `None` when the message
/// carries no headers, so plain messages stay property-free on the wire.
pub(crate) fn to_publish_properties(msg: &OutgoingMessage<'_>) -> Option<PublishProperties> {
    let headers = msg.headers();
    if headers.is_empty() {
        return None;
    }
    let mut properties = PublishProperties::default();
    for (name, value) in headers.iter() {
        let text = String::from_utf8_lossy(value).into_owned();
        match name {
            "content-type" => properties.content_type = Some(text),
            "reply-to" => properties.response_topic = Some(text),
            "correlation-id" => {
                properties.correlation_data = Some(Bytes::copy_from_slice(value));
            }
            other => properties.user_properties.push((other.to_owned(), text)),
        }
    }
    Some(properties)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn well_known_headers_ride_first_class_properties() {
        let mut headers = HeaderMap::new();
        headers.insert("content-type", "application/json");
        headers.insert("reply-to", "replies/1");
        headers.insert("correlation-id", "corr-1");
        headers.insert("x-tenant", "acme");
        let outgoing = OutgoingMessage::new("orders", b"{}".as_slice()).with_headers(headers);

        let properties = to_publish_properties(&outgoing).expect("properties built");
        assert_eq!(properties.content_type.as_deref(), Some("application/json"));
        assert_eq!(properties.response_topic.as_deref(), Some("replies/1"));
        assert_eq!(
            properties.correlation_data.as_deref(),
            Some(b"corr-1".as_slice())
        );
        assert_eq!(
            properties.user_properties,
            vec![("x-tenant".to_owned(), "acme".to_owned())]
        );
    }

    #[test]
    fn plain_messages_stay_property_free() {
        let outgoing = OutgoingMessage::new("orders", b"{}".as_slice());
        assert!(to_publish_properties(&outgoing).is_none());
    }
}
