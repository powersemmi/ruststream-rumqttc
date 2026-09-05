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

use crate::error::MqttError;
use crate::filter::Qos;
use crate::publisher::{QOS_HEADER, RETAIN_HEADER};

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

/// The per-message transport arguments an [`MqttPublishOptions`](crate::MqttPublishOptions)
/// adapter put on an outgoing message. Absent means "keep the publisher's policy value".
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub(crate) struct PerMessage {
    pub(crate) qos: Option<Qos>,
    pub(crate) retain: Option<bool>,
}

/// Reads the quality of service off its header, naming what the header takes and what arrived.
fn read_qos(value: &[u8]) -> Result<Qos, MqttError> {
    Qos::from_header(value).ok_or_else(|| MqttError::InvalidPublishArgument {
        header: QOS_HEADER,
        value: String::from_utf8_lossy(value).into_owned(),
        expected: "a quality of service (\"0\", \"1\", \"2\")",
    })
}

/// Reads the retain flag off its header, on the same terms.
fn read_retain(value: &[u8]) -> Result<bool, MqttError> {
    match value {
        b"true" => Ok(true),
        b"false" => Ok(false),
        _ => Err(MqttError::InvalidPublishArgument {
            header: RETAIN_HEADER,
            value: String::from_utf8_lossy(value).into_owned(),
            expected: "a retain flag (\"true\", \"false\")",
        }),
    }
}

/// Splits an outgoing message's headers into the per-message transport arguments and the wire
/// properties for everything else.
///
/// The two arguments are a channel between the adapter and this send path, so they are consumed
/// here and never travel as user properties. The properties are `None` when nothing else is left
/// to send, so a plain message stays property-free on the wire.
///
/// # Errors
///
/// Returns [`MqttError::InvalidPublishArgument`] when an argument header carries a value outside
/// its vocabulary. A publish that named a delivery guarantee is refused rather than sent under
/// the publisher's own, which would substitute a different guarantee without saying so.
pub(crate) fn to_wire_properties(
    msg: &OutgoingMessage<'_>,
) -> Result<(PerMessage, Option<PublishProperties>), MqttError> {
    let mut per_message = PerMessage::default();
    let mut properties = PublishProperties::default();
    let mut carries_properties = false;
    for (name, value) in msg.headers().iter() {
        match name {
            QOS_HEADER => {
                per_message.qos = Some(read_qos(value)?);
                continue;
            }
            RETAIN_HEADER => {
                per_message.retain = Some(read_retain(value)?);
                continue;
            }
            _ => {}
        }
        carries_properties = true;
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
    Ok((per_message, carries_properties.then_some(properties)))
}

/// Drops the per-message transport arguments from a header map, leaving what a subscriber sees.
/// The in-process test broker routes through it so its deliveries carry what the real transport
/// delivers; the arguments themselves say nothing without a protocol to apply them to.
///
/// # Errors
///
/// Reads each argument it drops, so an unreadable one is refused here exactly as the live
/// publisher refuses it, and a test meets the error a server would have produced.
#[cfg(feature = "testing")]
pub(crate) fn without_per_message(mut headers: HeaderMap) -> Result<HeaderMap, MqttError> {
    if let Some(value) = headers.remove(QOS_HEADER) {
        read_qos(&value)?;
    }
    if let Some(value) = headers.remove(RETAIN_HEADER) {
        read_retain(&value)?;
    }
    Ok(headers)
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

        let (per_message, properties) = to_wire_properties(&outgoing).expect("headers are read");
        assert_eq!(per_message, PerMessage::default());
        let properties = properties.expect("properties built");
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
        let (_, properties) = to_wire_properties(&outgoing).expect("headers are read");
        assert!(properties.is_none());
    }

    #[test]
    fn the_per_message_arguments_are_read_off_and_never_reach_the_wire() {
        let mut headers = HeaderMap::new();
        headers.insert(QOS_HEADER, Qos::ExactlyOnce.as_header());
        headers.insert(RETAIN_HEADER, "true");
        let outgoing = OutgoingMessage::new("states", b"online".as_slice()).with_headers(headers);

        let (per_message, properties) = to_wire_properties(&outgoing).expect("headers are read");
        assert_eq!(per_message.qos, Some(Qos::ExactlyOnce));
        assert_eq!(per_message.retain, Some(true));
        assert!(
            properties.is_none(),
            "a message carrying only the arguments stays property-free"
        );
    }

    /// The message has to name the header and quote the value: the publish is refused, and what
    /// the caller wrote is the only thing that says why.
    #[test]
    fn an_unreadable_quality_of_service_fails_the_publish_by_name() {
        let mut headers = HeaderMap::new();
        headers.insert(QOS_HEADER, "sometimes");
        let outgoing = OutgoingMessage::new("states", b"online".as_slice()).with_headers(headers);

        let error = to_wire_properties(&outgoing).expect_err("the value is outside the vocabulary");
        assert!(matches!(
            &error,
            MqttError::InvalidPublishArgument { header, value, .. }
                if *header == QOS_HEADER && value == "sometimes"
        ));
        assert_eq!(
            error.to_string(),
            "invalid mqtt publish argument: the mqtt-qos header carries a quality of service \
             (\"0\", \"1\", \"2\"); got \"sometimes\""
        );
    }

    #[test]
    fn an_unreadable_retain_flag_fails_the_publish_by_name() {
        let mut headers = HeaderMap::new();
        headers.insert(RETAIN_HEADER, "perhaps");
        let outgoing = OutgoingMessage::new("states", b"online".as_slice()).with_headers(headers);

        let error = to_wire_properties(&outgoing).expect_err("the value is outside the vocabulary");
        assert!(matches!(
            &error,
            MqttError::InvalidPublishArgument { header, value, .. }
                if *header == RETAIN_HEADER && value == "perhaps"
        ));
    }
}
