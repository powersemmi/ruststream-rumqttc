//! What this crate contributes to the generated `AsyncAPI` document: the `mqtt` protocol
//! bindings, the protocol version, and the expression a client reads a reply address from.
//!
//! Every value here is computed from the broker, the descriptor and the policy alone, without a
//! connection, which is what the document's own construction requires. The literals below are the
//! excerpts the documentation shows, so a change to either fails here first.

#![cfg(all(feature = "asyncapi", feature = "testing"))]

use std::time::Duration;

use ruststream::asyncapi::build_spec;
use ruststream::conformance::harness;
use ruststream::runtime::{Names, Outgoing, PublishContext, PublishTransform};
use ruststream_rumqttc::prelude::*;
use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Debug, Deserialize, Serialize, Outgoing)]
struct Telemetry {
    device: String,
    temperature: f64,
}

#[derive(Debug, Deserialize, Serialize, Outgoing)]
struct Pong {
    device: String,
}

#[derive(Debug, Serialize, Outgoing)]
#[outgoing(name = "alerts/overheat")]
struct Alert {
    device: String,
}

/// The slot the alert leaves through, which is where a send operation comes from: a reply has
/// none of its own.
#[derive(OutSlot)]
#[publishes(Alert)]
struct Alerts;

/// A fleet subscription: a filter, a share group, and the quality of service its deliveries are
/// settled under.
#[subscriber(MqttFilter::new("devices/+/telemetry").qos(Qos::ExactlyOnce).shared("workers"))]
async fn collect(
    telemetry: &Telemetry,
    Out(alerts): Out<impl Publisher, Alerts>,
) -> HandlerOutcome {
    if telemetry.temperature > 30.0 {
        let alert = Alert {
            device: telemetry.device.clone(),
        };
        if alerts.message(&alert).publish().await.is_err() {
            return HandlerOutcome::retry();
        }
    }
    HandlerOutcome::ack()
}

/// A responder whose answer goes to the topic the request named, which is the pattern the reply
/// address expression describes.
#[subscriber(MqttTopic::new("devices/dev42/ping"), publish("devices/dev42/pong"))]
async fn answer(telemetry: &Telemetry) -> Pong {
    Pong {
        device: telemetry.device.clone(),
    }
}

/// Answers on the request's own response topic, which arrives as the `reply-to` header. A
/// transform that names the destination per delivery is what puts the expression in the document.
struct ToResponseTopic;

impl<C, Options> PublishTransform<ForReply<C>, Options> for ToResponseTopic {
    type Destination = Names;

    fn apply(
        &self,
        out: &mut Outgoing<'_>,
        _options: &mut Option<Options>,
        cx: &PublishContext<'_, C>,
    ) {
        if let Some(topic) = cx.headers().get_str("reply-to") {
            out.set_name(topic.to_owned());
        }
    }
}

/// The session a client opens, as the document reports it. No credential appears: the binding
/// carries the client identity and the session settings, and the last will contributes its
/// coordinates without its payload.
// --8<-- [start:server_binding]
const SERVER_BINDING: &str = r#"{
  "bindingVersion": "0.2.0",
  "clientId": "telemetry-svc",
  "cleanSession": false,
  "keepAlive": 30,
  "sessionExpiryInterval": 3600,
  "maximumPacketSize": 1048576,
  "lastWill": { "topic": "devices/svc/status", "qos": 1, "retain": true }
}"#;
// --8<-- [end:server_binding]

/// What one subscription adds to its receive operation: the quality of service it reads at.
// --8<-- [start:operation_binding]
const RECEIVE_BINDING: &str = r#"{ "bindingVersion": "0.2.0", "qos": 2 }"#;
// --8<-- [end:operation_binding]

/// What one publish policy adds to its send operation: both arguments MQTT carries on a PUBLISH
/// packet.
// --8<-- [start:send_binding]
const SEND_BINDING: &str = r#"{ "bindingVersion": "0.2.0", "qos": 1, "retain": true }"#;
// --8<-- [end:send_binding]

/// The MQTT 5 properties a message is mapped through, in both directions.
// --8<-- [start:message_binding]
const MESSAGE_BINDING: &str = r#"{
  "bindingVersion": "0.2.0",
  "payloadFormatIndicator": 0,
  "correlationData": {
    "type": "string",
    "description": "The correlation-id header, carried in the MQTT 5 Correlation Data property."
  },
  "responseTopic": {
    "type": "string",
    "description": "The reply-to header, carried in the MQTT 5 Response Topic property."
  }
}"#;
// --8<-- [end:message_binding]

fn broker() -> MqttBroker {
    MqttBroker::new("mqtt://alice:hunter2@localhost:1883", "telemetry-svc")
        .credentials("alice", "hunter2")
        .keep_alive(Duration::from_secs(30))
        .clean_start(false)
        .session_expiry(Duration::from_secs(3600))
        .last_will(
            "devices/svc/status",
            b"offline".to_vec(),
            Qos::AtLeastOnce,
            true,
        )
}

/// The whole document of a service on this broker, as JSON.
fn document() -> Value {
    let app = RustStream::new(AppInfo::new("telemetry", "1.0.0")).with_broker_labeled(
        "mqtt",
        broker(),
        |b| {
            b.include(collect)
                .out(Alerts, Publish::default().retain(true))
                .out_retry(Publish::default())
                .to("devices/retry/telemetry")
                .build();
            b.include(answer)
                .out_reply(Publish::default())
                .transform(ToResponseTopic);
        },
    );
    let json = build_spec(&app)
        .to_json()
        .expect("the generated document must serialize");
    serde_json::from_str(&json).expect("the generated document is JSON")
}

fn expected(excerpt: &str) -> Value {
    serde_json::from_str(excerpt).expect("the documented excerpt is JSON")
}

#[test]
fn the_server_reports_the_protocol_version_and_the_session_it_opens() {
    let document = document();
    let server = &document["servers"]["mqtt"];

    assert_eq!(server["protocolVersion"], "5");
    assert_eq!(server["bindings"]["mqtt"], expected(SERVER_BINDING));
}

#[test]
fn a_subscription_reports_the_quality_of_service_it_reads_at() {
    let document = document();
    let operation = &document["operations"]["receive_devices___telemetry"];

    assert_eq!(operation["bindings"]["mqtt"], expected(RECEIVE_BINDING));
}

#[test]
fn a_publish_policy_reports_both_arguments_of_its_packets() {
    let document = document();
    let operation = &document["operations"]["send_devices___telemetry_alerts_overheat"];

    assert_eq!(operation["bindings"]["mqtt"], expected(SEND_BINDING));
}

#[test]
fn a_message_reports_the_properties_it_is_mapped_through() {
    let document = document();
    let message = &document["components"]["messages"]["Telemetry"];

    assert_eq!(message["bindings"]["mqtt"], expected(MESSAGE_BINDING));
}

/// A reply whose destination a transform names per delivery has no fixed address, so the document
/// says where a client reads it instead.
#[test]
fn a_reply_named_per_delivery_reports_where_its_address_is_read() {
    let document = document();
    let operation = &document["operations"]["receive_devices_dev42_ping"];

    assert_eq!(
        operation["reply"]["address"]["location"],
        "$message.header#/reply-to"
    );
}

/// The scan the framework ships: a broker configured with a known password, and a document that
/// must not carry it anywhere.
#[test]
fn the_document_carries_no_credentials() {
    harness::describes_without_credentials(
        &broker(),
        &MqttFilter::new("devices/+/telemetry").shared("workers"),
        "hunter2",
    );
}
