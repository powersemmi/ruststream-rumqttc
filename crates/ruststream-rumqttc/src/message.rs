//! [`MqttMessage`] and the mapping between `RustStream` headers and MQTT 5 properties.
//!
//! User properties carry headers natively; the well-known `content-type`, `reply-to`, and
//! `correlation-id` headers ride the matching first-class MQTT 5 properties, so no envelope
//! format is invented and non-Rust peers see plain MQTT messages.

use std::str;

use bytes::Bytes;
use rumqttc::v5::mqttbytes::QoS;
use rumqttc::v5::mqttbytes::v5::{Publish, PublishProperties};
use ruststream::{AckError, HeaderMap, IncomingMessage, OutgoingMessage, Str};

use crate::broker::Link;
use crate::error::MqttError;
use crate::filter::Qos;
#[cfg(feature = "testing")]
use crate::in_process::InFlight;

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
    qos: Qos,
    /// `None` when this delivery carries no acknowledgement: `QoS` 0, or a fanned-out copy on
    /// an overlapping filter (the wire ack belongs to exactly one delivery).
    acker: Option<(Link, Publish)>,
    /// The test harness's count of this delivery, released when the delivery is dropped, settled
    /// or not. Only a delivery of the in-process mode carries one, and only with the `testing`
    /// feature is the field there at all.
    #[cfg(feature = "testing")]
    in_flight: Option<InFlight>,
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
    pub(crate) fn new(topic: String, publish: &Publish, link: Option<Link>) -> Self {
        let acker = match publish.qos {
            QoS::AtMostOnce => None,
            _ => link.map(|link| (link, publish.clone())),
        };
        Self {
            payload: publish.payload.clone(),
            headers: headers_of(publish),
            topic,
            qos: Qos::from_client(publish.qos),
            acker,
            #[cfg(feature = "testing")]
            in_flight: None,
        }
    }

    /// This delivery, counted by the test harness until it is dropped.
    #[cfg(feature = "testing")]
    pub(crate) fn counted(mut self, in_flight: Option<InFlight>) -> Self {
        self.in_flight = in_flight;
        self
    }

    /// This delivery, no longer counted by the test harness while it is held.
    #[cfg(feature = "testing")]
    pub(crate) fn uncounted(mut self) -> Self {
        if let Some(in_flight) = &mut self.in_flight {
            in_flight.suspend();
        }
        self
    }

    /// A held delivery a subscription takes, counted again until it is dropped.
    #[cfg(feature = "testing")]
    pub(crate) fn recounted(mut self) -> Self {
        if let Some(in_flight) = &mut self.in_flight {
            in_flight.resume();
        }
        self
    }

    /// The topic this message was published to (the real topic, never a `$share` filter).
    #[must_use]
    pub fn topic(&self) -> &str {
        &self.topic
    }

    /// The quality of service this message was delivered at: the lesser of the one it was
    /// published with and the one its subscription asked for.
    ///
    /// A delivery at [`Qos::AtMostOnce`] carries no acknowledgement, so `ack` reports
    /// [`AckError::Unsupported`] for it.
    #[must_use]
    pub const fn qos(&self) -> Qos {
        self.qos
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
        let Some((link, publish)) = self.acker else {
            return Err(AckError::Unsupported);
        };
        link.ack(&publish).await
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

/// The headers a PUBLISH packet carries: its user properties, plus the well-known headers its
/// first-class properties hold.
pub(crate) fn headers_of(publish: &Publish) -> HeaderMap {
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
    headers
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
///
/// Every header but `correlation-id` becomes an MQTT string, so a header whose name or value is
/// not [MQTT text](is_mqtt_text) refuses the publish: the packet would otherwise reach the server
/// rewritten, or not at all.
pub(crate) fn to_wire_properties<Payload>(
    msg: &OutgoingMessage<'_, Payload>,
) -> Result<Option<PublishProperties>, MqttError> {
    let mut properties = PublishProperties::default();
    let mut carries_properties = false;
    for (name, value) in msg.headers().iter() {
        carries_properties = true;
        // Binary data on the wire, the one property that takes any bytes.
        if name == "correlation-id" {
            properties.correlation_data = Some(Bytes::copy_from_slice(value));
            continue;
        }
        let text = match str::from_utf8(value) {
            Ok(text) if is_mqtt_text(name) && is_mqtt_text(text) => text.to_owned(),
            _ => {
                return Err(MqttError::Publish {
                    topic: msg.name().to_owned(),
                    reason: format!(
                        "header {name:?} is not text an MQTT property carries: its name and value \
                         must be UTF-8 with no control character and no Unicode non-character"
                    ),
                });
            }
        };
        match name {
            "content-type" => {
                properties.payload_format_indicator = Some(u8::from(is_text_media_type(&text)));
                properties.content_type = Some(text);
            }
            "reply-to" => properties.response_topic = Some(text),
            other => properties.user_properties.push((other.to_owned(), text)),
        }
    }
    Ok(carries_properties.then_some(properties))
}

/// Whether `text` is a string an MQTT packet carries as it is.
///
/// MQTT 5 forbids U+0000 in a string and tells senders to keep out the other control characters
/// and the Unicode non-characters (section 1.5.4). A receiver may close the connection on any of
/// them, and Mosquitto does, taking every message in flight on that connection with it.
pub(crate) fn is_mqtt_text(text: &str) -> bool {
    // Printable ASCII, which almost every header and topic is, answers in one pass over the
    // bytes; only text beyond it is decoded character by character.
    text.bytes().all(|byte| matches!(byte, 0x20..=0x7e))
        || text.chars().all(|c| !c.is_control() && !is_noncharacter(c))
}

/// The Unicode non-characters: U+FDD0 to U+FDEF, and the last two code points of every plane.
const fn is_noncharacter(c: char) -> bool {
    let code = c as u32;
    (code >= 0xFDD0 && code <= 0xFDEF) || code & 0xFFFE == 0xFFFE
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
        let outgoing: OutgoingMessage<'_> =
            OutgoingMessage::new("orders", b"{}".as_slice()).with_headers(headers);

        let properties = to_wire_properties(&outgoing)
            .expect("the headers are MQTT text")
            .expect("properties built");
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
        let outgoing: OutgoingMessage<'_> = OutgoingMessage::new("orders", b"{}".as_slice());
        assert!(
            to_wire_properties(&outgoing)
                .expect("no headers to refuse")
                .is_none()
        );
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
            let outgoing: OutgoingMessage<'_> =
                OutgoingMessage::new("orders", b"{}".as_slice()).with_headers(headers);
            let properties = to_wire_properties(&outgoing)
                .expect("the headers are MQTT text")
                .expect("properties built");
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
        let outgoing: OutgoingMessage<'_> =
            OutgoingMessage::new("orders", b"{}".as_slice()).with_headers(headers);

        let properties = to_wire_properties(&outgoing)
            .expect("the headers are MQTT text")
            .expect("properties built");
        assert_eq!(properties.payload_format_indicator, None);
    }

    /// Every header a message carries is a user property or a first-class one. The delivery
    /// arguments are not among them: they are fields of the packet, and a header named after one
    /// is a header like any other.
    #[test]
    fn nothing_is_read_off_the_headers_on_the_way_to_the_wire() {
        let mut headers = HeaderMap::new();
        headers.insert("mqtt-qos", "2");
        let outgoing: OutgoingMessage<'_> =
            OutgoingMessage::new("states", b"online".as_slice()).with_headers(headers);

        let properties = to_wire_properties(&outgoing)
            .expect("the headers are MQTT text")
            .expect("properties built");
        assert_eq!(
            properties.user_properties,
            vec![("mqtt-qos".to_owned(), "2".to_owned())]
        );
    }

    /// A header an MQTT string cannot carry refuses the publish rather than reaching the server
    /// rewritten, or reaching it at all: a server may close the connection on such a packet.
    #[test]
    fn a_header_that_is_not_mqtt_text_refuses_the_publish() {
        for (name, value) in [
            ("x-binary", &[0x00, 0x80, 0xfe, 0xff][..]),
            ("x-line", b"one\r\ntwo".as_slice()),
            ("x-tab", b"a\tb".as_slice()),
            ("x-nul", b"a\0b".as_slice()),
            ("x-noncharacter", "a\u{fffe}b".as_bytes()),
            ("x-noncharacter", "a\u{fdd0}b".as_bytes()),
            ("content-type", b"text/plain\n".as_slice()),
            ("reply-to", &[0xc3][..]),
            ("x-name\n", b"fine".as_slice()),
        ] {
            let mut headers = HeaderMap::new();
            headers.insert(name.to_owned(), Bytes::copy_from_slice(value));
            let outgoing: OutgoingMessage<'_> =
                OutgoingMessage::new("orders", b"{}".as_slice()).with_headers(headers);
            let err = to_wire_properties(&outgoing)
                .expect_err("a header MQTT cannot carry is refused, never rewritten");
            assert!(
                matches!(&err, MqttError::Publish { topic, .. } if topic == "orders"),
                "{name:?}: {err}"
            );
        }
    }

    /// The correlation data is binary on the wire, so any bytes ride it unchanged; and text MQTT
    /// carries, non-ASCII included, is accepted as it is.
    #[test]
    fn binary_correlation_data_and_unicode_text_are_carried() {
        let mut headers = HeaderMap::new();
        headers.insert("correlation-id", Bytes::from_static(&[0x00, 0x80, 0xff]));
        headers.insert("x-city", "Zürich 東京");
        let outgoing: OutgoingMessage<'_> =
            OutgoingMessage::new("orders", b"{}".as_slice()).with_headers(headers);

        let properties = to_wire_properties(&outgoing)
            .expect("the headers are MQTT text")
            .expect("properties built");
        assert_eq!(
            properties.correlation_data.as_deref(),
            Some([0x00, 0x80, 0xff].as_slice())
        );
        assert_eq!(
            properties.user_properties,
            vec![("x-city".to_owned(), "Zürich 東京".to_owned())]
        );
    }
}
