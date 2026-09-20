//! [`MqttMessage`] and the mapping between `RustStream` headers and MQTT 5 properties.
//!
//! User properties carry headers natively; the well-known `content-type`, `reply-to`, and
//! `correlation-id` headers ride the matching first-class MQTT 5 properties, so no envelope
//! format is invented and non-Rust peers see plain MQTT messages.

use bytes::Bytes;
use rumqttc::v5::AsyncClient;
use rumqttc::v5::mqttbytes::QoS;
use rumqttc::v5::mqttbytes::v5::{Publish, PublishProperties};
use ruststream::{AckError, HeaderMap, IncomingMessage, OutgoingMessage, Str};

/// The header keys the MQTT 5 first-class properties map onto, built once so that mapping a
/// delivery copies nothing for them.
const CONTENT_TYPE: Str = Str::from_static("content-type");
const REPLY_TO: Str = Str::from_static("reply-to");
const CORRELATION_ID: Str = Str::from_static("correlation-id");

/// A message delivered by an [`MqttSubscriber`](crate::MqttSubscriber).
///
/// `ack` acknowledges through the protocol for `QoS` 1 (`PUBACK`) and `QoS` 2 (`PUBREC`, with the
/// client completing the handshake); `QoS` 0 deliveries report
/// [`AckError::Unsupported`]. MQTT has no negative acknowledgement, so `nack(requeue = true)`
/// reports [`AckError::Unsupported`] as well - unacknowledged messages redeliver when the
/// session resumes - and `nack(requeue = false)` acknowledges (dropping is the only terminal
/// outcome the protocol offers).
///
/// A handler's `HandlerOutcome::retry()` settles through that refused negative acknowledgement, so
/// it does not retry inside the live connection: the delivery stays unacknowledged and comes back
/// only when a persistent session resumes. `retry_after` with a retry publisher is the outcome
/// that retries within the session; the crate overview's acknowledgement section spells both
/// out.
///
/// A delivery reports no redelivery count. The protocol carries a duplicate flag and no counter,
/// so a registration's cap is counted on the framework's retry-count header instead, which the
/// copies the runtime publishes carry.
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
                headers.insert(CONTENT_TYPE, content_type.clone());
            }
            if let Some(response_topic) = &properties.response_topic {
                headers.insert(REPLY_TO, response_topic.clone());
            }
            if let Some(correlation) = &properties.correlation_data {
                headers.insert(CORRELATION_ID, correlation.clone());
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

/// Whether a media type names text on the wire.
///
/// The MQTT 5 payload format indicator is a two-valued answer - unspecified bytes or UTF-8 - so
/// the question is only whether the media type is a textual one. JSON is, whatever vendor prefix
/// it carries, and so is every `text/` subtype; everything else is bytes as far as the protocol
/// is concerned. Parameters after `;` (a charset) say nothing about that and are dropped.
fn is_text_media_type(content_type: &str) -> bool {
    let media = content_type
        .split(';')
        .next()
        .unwrap_or_default()
        .trim()
        .to_ascii_lowercase();
    media.starts_with("text/") || media == "application/json" || media.ends_with("+json")
}

/// Builds the wire properties of an outgoing message from its headers.
///
/// `None` when the message carries no headers at all, so a plain message stays property-free on
/// the wire and takes the protocol's own defaults. The quality of service and the retain flag are
/// not here: they are protocol fields of the PUBLISH packet, carried as
/// [`MqttPublishOptions`](crate::MqttPublishOptions) and resolved before the packet is built.
///
/// The payload format indicator is decided here rather than declared anywhere, because it follows
/// the media type this message carries in its `content-type` header, which only the sender puts
/// there. A message whose media type is textual is published as UTF-8 (`1`), every other one as
/// unspecified bytes (`0`), and the same header fills the MQTT 5 content type property, so a
/// non-Rust peer reads both from the packet. A message that names no media type declares neither.
pub(crate) fn to_wire_properties(msg: &OutgoingMessage<'_>) -> Option<PublishProperties> {
    let mut properties = PublishProperties::default();
    let mut carries_properties = false;
    for (name, value) in msg.headers().iter() {
        carries_properties = true;
        let text = String::from_utf8_lossy(value).into_owned();
        match name {
            "content-type" => {
                properties.payload_format_indicator = Some(u8::from(is_text_media_type(&text)));
                properties.content_type = Some(text);
            }
            "reply-to" => properties.response_topic = Some(text),
            "correlation-id" => {
                properties.correlation_data = Some(Bytes::copy_from_slice(value));
            }
            other => properties.user_properties.push((other.to_owned(), text)),
        }
    }
    carries_properties.then_some(properties)
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

        let properties = to_wire_properties(&outgoing).expect("properties built");
        assert_eq!(properties.content_type.as_deref(), Some("application/json"));
        assert_eq!(properties.payload_format_indicator, Some(1));
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
        assert!(to_wire_properties(&outgoing).is_none());
    }

    /// The indicator follows the media type, and the protocol has only two answers: UTF-8 or
    /// unspecified bytes. A vendor JSON type is still JSON; a binary codec's type is not.
    #[test]
    fn the_payload_format_follows_the_media_type() {
        for (content_type, expected) in [
            ("application/json", 1),
            ("application/json; charset=utf-8", 1),
            ("application/vnd.acme.order+json", 1),
            ("text/plain", 1),
            ("TEXT/CSV", 1),
            ("application/cbor", 0),
            ("application/msgpack", 0),
            ("application/octet-stream", 0),
        ] {
            let mut headers = HeaderMap::new();
            headers.insert("content-type", content_type);
            let outgoing = OutgoingMessage::new("orders", b"{}".as_slice()).with_headers(headers);
            let properties = to_wire_properties(&outgoing).expect("properties built");
            assert_eq!(
                properties.payload_format_indicator,
                Some(expected),
                "{content_type} is {}",
                if expected == 1 { "text" } else { "bytes" }
            );
        }
    }

    /// A message with no media type says nothing about its format, which is the protocol's own
    /// default of unspecified bytes.
    #[test]
    fn a_message_without_a_media_type_declares_no_payload_format() {
        let mut headers = HeaderMap::new();
        headers.insert("x-tenant", "acme");
        let outgoing = OutgoingMessage::new("orders", b"{}".as_slice()).with_headers(headers);

        let properties = to_wire_properties(&outgoing).expect("properties built");
        assert_eq!(properties.payload_format_indicator, None);
    }

    /// Every header a message carries is a user property or a first-class one. The delivery
    /// arguments are not among them: they are fields of the packet, and a header named after one
    /// is a header like any other.
    #[test]
    fn nothing_is_read_off_the_headers_on_the_way_to_the_wire() {
        let mut headers = HeaderMap::new();
        headers.insert("mqtt-qos", "2");
        let outgoing = OutgoingMessage::new("states", b"online".as_slice()).with_headers(headers);

        let properties = to_wire_properties(&outgoing).expect("properties built");
        assert_eq!(
            properties.user_properties,
            vec![("mqtt-qos".to_owned(), "2".to_owned())]
        );
    }
}
