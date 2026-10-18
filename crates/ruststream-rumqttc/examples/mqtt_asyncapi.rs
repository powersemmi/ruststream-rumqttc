//! The `AsyncAPI` document of an MQTT service: the `mqtt` bindings describe the session the
//! client opens, the quality of service of each operation and the MQTT 5 properties of each
//! message. The tests read the document the app generates.
//!
//! ```text
//! cargo run --example mqtt_asyncapi --features asyncapi -- asyncapi gen
//! cargo test --example mqtt_asyncapi --features asyncapi
//! ```

// A plain build compiles the service; the tests that use it exist only in the test build.
#![cfg_attr(not(test), allow(dead_code, unused_imports))]

use std::env;
use std::time::Duration;

// The derive and the value a publish transform reads share the name in different namespaces: the
// prelude carries the macro `ruststream::Outgoing`, and this is the type `runtime::Outgoing`.
use ruststream::runtime::{Names, Outgoing, PublishContext};
use ruststream_rumqttc::prelude::*;
use serde::{Deserialize, Serialize};

// --8<-- [start:messages]
/// What every device publishes; a subscription reads it.
#[derive(Debug, Deserialize, Serialize, Outgoing)]
struct Telemetry {
    device: String,
    temperature: f64,
}

/// What the service publishes through its slot when a device runs hot.
#[derive(Debug, Serialize, Outgoing)]
#[outgoing(name = "alerts/overheat")]
struct Alert {
    device: String,
}
// --8<-- [end:messages]

/// The answer to a ping; the request decides where it goes.
#[derive(Debug, Serialize, Outgoing)]
struct Pong {
    device: String,
}

#[derive(OutSlot)]
#[publishes(Alert)]
struct Alerts;

// --8<-- [start:operations]
/// Reads the fleet at `QoS` 2 as a member of `workers`, and raises an alert through its slot.
#[subscriber(MqttFilter::new("devices/+/telemetry").qos(Qos::ExactlyOnce).shared("workers"))]
async fn collect(
    telemetry: &Telemetry,
    Out(alerts): Out<impl Publisher, Alerts>,
) -> HandlerOutcome {
    if telemetry.temperature > 30.0 {
        let alert = Alert {
            device: telemetry.device.clone(),
        };
        if let Err(err) = alerts.message(&alert).publish().await {
            tracing::warn!(subscription = "devices/+/telemetry", error = %err, "alert not published");
            return HandlerOutcome::retry();
        }
    }
    HandlerOutcome::ack()
}
// --8<-- [end:operations]

// --8<-- [start:replies]
/// Answers on the topic the request names in its response topic, the `reply-to` header.
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

/// A request without a response topic is answered on `devices/dev42/pong`.
#[subscriber(MqttTopic::new("devices/dev42/ping"), reply("devices/dev42/pong"))]
async fn answer(ping: &Telemetry) -> Pong {
    Pong {
        device: ping.device.clone(),
    }
}
// --8<-- [end:replies]

// --8<-- [start:server]
/// The production server, with a persistent session and a last will.
fn broker(password: &str) -> MqttBroker {
    MqttBroker::new("mqtt://mqtt.internal:1883", "telemetry-svc")
        .credentials("telemetry-svc", password)
        .keep_alive(Duration::from_secs(30))
        .clean_start(false)
        .session_expiry(Duration::from_hours(1))
        .last_will(
            "devices/svc/status",
            b"offline".to_vec(),
            Qos::AtLeastOnce,
            true,
        )
}
// --8<-- [end:server]

// --8<-- [start:app]
fn service(broker: MqttBroker) -> impl App<State = ()> {
    RustStream::new(AppInfo::new("telemetry", "1.0.0")).with_broker_labeled("mqtt", broker, |b| {
        b.include(collect)
            .out(Alerts, Publish::default().retain(true))
            .out_retry(Publish::default())
            .to("devices/retry/telemetry")
            .build();
        b.include(answer)
            .out_reply(Publish::default())
            .transform(ToResponseTopic);
    })
}

#[ruststream::app]
fn app() -> impl App<State = ()> {
    let password = env::var("MQTT_PASSWORD").unwrap_or_default();
    service(broker(&password))
}
// --8<-- [end:app]

#[cfg(test)]
mod tests {
    use std::error::Error;
    use std::hash::{BuildHasher, Hasher, RandomState};

    use ruststream::asyncapi::build_spec;
    use serde_json::{Value, json};

    use super::{broker, service};

    /// A secret made up at run time, so none is written into the source.
    fn made_up_secret() -> String {
        RandomState::new().build_hasher().finish().to_string()
    }

    fn document(password: &str) -> Result<Value, Box<dyn Error>> {
        Ok(serde_json::from_str(
            &build_spec(&service(broker(password))).to_json()?,
        )?)
    }

    #[test]
    fn every_part_of_the_document_speaks_mqtt() -> Result<(), Box<dyn Error>> {
        let spec = document(&made_up_secret())?;

        for bindings in [
            &spec["servers"]["mqtt"]["bindings"],
            &spec["operations"]["receive_devices___telemetry"]["bindings"],
            &spec["components"]["messages"]["Telemetry"]["bindings"],
        ] {
            assert_eq!(bindings["mqtt"]["bindingVersion"], "0.2.0", "{bindings}");
        }
        Ok(())
    }

    #[test]
    fn the_server_reports_the_session_the_client_opens() -> Result<(), Box<dyn Error>> {
        let spec = document(&made_up_secret())?;
        let server = &spec["servers"]["mqtt"];

        assert_eq!(server["host"], "mqtt.internal:1883");
        assert_eq!(server["protocol"], "mqtt");
        assert_eq!(server["protocolVersion"], "5");
        assert_eq!(
            server["bindings"]["mqtt"],
            json!({
                "bindingVersion": "0.2.0",
                "clientId": "telemetry-svc",
                "cleanSession": false,
                "keepAlive": 30,
                "sessionExpiryInterval": 3600,
                "maximumPacketSize": 1_048_576,
                "lastWill": { "topic": "devices/svc/status", "qos": 1, "retain": true },
            })
        );
        Ok(())
    }

    #[test]
    fn the_document_carries_no_credential() -> Result<(), Box<dyn Error>> {
        let password = made_up_secret();
        let text = document(&password)?.to_string();

        assert!(!text.contains(&password), "{text}");
        assert!(
            !text.contains("offline"),
            "the will's payload stays out: {text}"
        );
        Ok(())
    }

    #[test]
    fn each_operation_reports_its_quality_of_service() -> Result<(), Box<dyn Error>> {
        let spec = document(&made_up_secret())?;
        let operations = &spec["operations"];

        assert_eq!(
            operations["receive_devices___telemetry"]["bindings"]["mqtt"],
            json!({ "bindingVersion": "0.2.0", "qos": 2 })
        );
        assert_eq!(
            operations["send_devices___telemetry_alerts_overheat"]["bindings"]["mqtt"],
            json!({ "bindingVersion": "0.2.0", "qos": 1, "retain": true })
        );
        Ok(())
    }

    #[test]
    fn each_message_reports_the_properties_it_rides() -> Result<(), Box<dyn Error>> {
        let spec = document(&made_up_secret())?;
        let messages = &spec["components"]["messages"];

        let read = &messages["Telemetry"]["bindings"]["mqtt"];
        assert_eq!(read["correlationData"]["type"], "string");
        assert_eq!(read["responseTopic"]["type"], "string");
        for published in ["Alert", "Pong"] {
            let binding = &messages[published]["bindings"]["mqtt"];
            assert_eq!(binding["correlationData"]["type"], "string", "{published}");
            assert!(binding["responseTopic"].is_null(), "{published}: {binding}");
        }
        Ok(())
    }

    #[test]
    fn a_reply_named_per_request_points_at_the_response_topic() -> Result<(), Box<dyn Error>> {
        let spec = document(&made_up_secret())?;

        assert_eq!(
            spec["operations"]["receive_devices_dev42_ping"]["reply"]["address"]["location"],
            "$message.header#/reply-to"
        );
        Ok(())
    }
}
