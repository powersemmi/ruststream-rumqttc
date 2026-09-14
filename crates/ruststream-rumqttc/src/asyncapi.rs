//! The `mqtt` protocol bindings this crate contributes to the generated `AsyncAPI` document.
//!
//! The specification's `mqtt` binding (version 0.2.0, the one that replaced the deprecated
//! `mqtt5`) has room for the session a client opens, the quality of service an operation runs at,
//! and the MQTT 5 properties a message carries. Each body here is built from what the broker, the
//! subscription descriptor or the publish policy already holds, with no connection and no
//! credential: the document is generated before anything connects, and it is published and
//! shared.

use ruststream::asyncapi::{Binding, Bindings};
use serde::Serialize;

use crate::filter::Qos;

/// The binding key and the version of it this crate writes.
pub(crate) const PROTOCOL: &str = "mqtt";
const VERSION: &str = "0.2.0";

/// Wraps one body into the crate's single binding, or into nothing.
///
/// A binding that fails to build is a binding the document goes without: a broker never holds up
/// a service over a description of itself.
fn one<T: Serialize>(body: &T) -> Bindings {
    Binding::new(PROTOCOL, VERSION, body)
        .map_or_else(|_| Bindings::new(), |b| Bindings::new().with(b))
}

/// The session a client opens, as the server object of the binding describes it.
#[derive(Debug, Serialize)]
pub(crate) struct MqttServer {
    #[serde(rename = "clientId")]
    pub(crate) client_id: String,
    #[serde(rename = "cleanSession", skip_serializing_if = "Option::is_none")]
    pub(crate) clean_session: Option<bool>,
    #[serde(rename = "lastWill", skip_serializing_if = "Option::is_none")]
    pub(crate) last_will: Option<MqttLastWill>,
    #[serde(rename = "keepAlive", skip_serializing_if = "Option::is_none")]
    pub(crate) keep_alive: Option<u64>,
    #[serde(
        rename = "sessionExpiryInterval",
        skip_serializing_if = "Option::is_none"
    )]
    pub(crate) session_expiry_interval: Option<u32>,
    #[serde(rename = "maximumPacketSize")]
    pub(crate) maximum_packet_size: u32,
}

/// The last will, without its payload: a will message is content, not a coordinate, and content
/// that may be internal has no place in a document a team publishes.
#[derive(Debug, Serialize)]
pub(crate) struct MqttLastWill {
    pub(crate) topic: String,
    pub(crate) qos: u8,
    pub(crate) retain: bool,
}

/// What one operation runs at. `retain` is a property of a publish, so a receive operation leaves
/// it out rather than reporting a default nobody set.
#[derive(Debug, Serialize)]
struct MqttOperation {
    qos: u8,
    #[serde(skip_serializing_if = "Option::is_none")]
    retain: Option<bool>,
}

/// The MQTT 5 properties this crate maps a message onto: `correlation-id` rides the Correlation
/// Data property and `reply-to` rides the Response Topic property, in both directions.
///
/// `responseTopic` describes an incoming delivery only. The property is one a requester sets on
/// its own request, so a message a service publishes carries none, and the binding leaves the
/// field out there rather than describing a property the packet will not have. Where a reply goes
/// is the operation's `reply` object, and under a naming transform its `reply.address.location`.
///
/// `payloadFormatIndicator` is deliberately absent. The indicator follows the media type of the
/// message, which the codec of the publish position produces, and neither the subscription
/// descriptor nor the publish policy is handed that codec - the document is built from
/// declarations, and the codec is resolved at the mount site. Reporting a fixed value here would
/// describe every message by the one the crate happened to pick, so the packet decides it instead
/// (see [`to_wire_properties`](crate::message)) and the document reports the media type itself
/// through `contentType`, which the core fills from the codec.
#[derive(Debug, Serialize)]
struct MqttMessageBinding {
    #[serde(rename = "correlationData")]
    correlation_data: StringSchema,
    #[serde(rename = "responseTopic", skip_serializing_if = "Option::is_none")]
    response_topic: Option<StringSchema>,
}

/// The shape of a property the crate carries as a header.
#[derive(Debug, Serialize)]
struct StringSchema {
    #[serde(rename = "type")]
    kind: &'static str,
    description: &'static str,
}

/// The server binding of one broker configuration.
pub(crate) fn server(server: &MqttServer) -> Bindings {
    one(server)
}

/// The receive operation of a subscription running at `qos`.
pub(crate) fn receive_operation(qos: Qos) -> Bindings {
    one(&MqttOperation {
        qos: qos.level(),
        retain: None,
    })
}

/// The send operation of a publish policy.
pub(crate) fn send_operation(qos: Qos, retain: bool) -> Bindings {
    one(&MqttOperation {
        qos: qos.level(),
        retain: Some(retain),
    })
}

/// The properties every delivery a subscription reads is mapped through, the response topic each
/// requester sets among them.
pub(crate) fn message() -> Bindings {
    mapped_properties(Some(StringSchema {
        kind: "string",
        description: "The reply-to header, carried in the MQTT 5 Response Topic property.",
    }))
}

/// The properties every message a publish position sends is mapped through, which is the
/// correlation data alone: a request sets the response topic, and a message a service sends is
/// not that request.
pub(crate) fn publish_message() -> Bindings {
    mapped_properties(None)
}

/// The one message binding both directions build, differing only in the property a service sends
/// nothing in.
fn mapped_properties(response_topic: Option<StringSchema>) -> Bindings {
    one(&MqttMessageBinding {
        correlation_data: StringSchema {
            kind: "string",
            description: "The correlation-id header, carried in the MQTT 5 Correlation Data \
                          property.",
        },
        response_topic,
    })
}

/// Where a client reads the address of an answer this broker routes.
///
/// The crate maps the Response Topic property onto the `reply-to` header in both directions, so
/// this is the expression a reader of the document follows to find where a reply goes.
pub(crate) const REPLY_ADDRESS_LOCATION: &str = "$message.header#/reply-to";
