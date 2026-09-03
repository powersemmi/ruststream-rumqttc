//! Handlers on this broker, driven through the framework's own surfaces rather than the broker
//! SPI: a `#[subscriber]` body runs on the in-process transport under `TestApp`, and the crate's
//! per-message publish steps are reached through an injected `Out` slot.
//!
//! The live suite (`integration_mqtt.rs`) covers the transport; this file covers the seam
//! between the crate and the framework's dispatch and injection paths, which needs no server.

#![cfg(feature = "testing")]

use ruststream::testing::TestApp;
use ruststream_rumqttc::prelude::*;
use ruststream_rumqttc::testing::{MqttTestBroker, MqttTestPublish};
use serde::{Deserialize, Serialize};

// The attribute takes the topic as a literal; the assertions address the same subscription.
const TELEMETRY: &str = "devices/dev42/telemetry";

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
struct Telemetry {
    device: String,
    temperature: f64,
}

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
#[outgoing(name = "alerts")]
struct Alert {
    device: String,
}

/// Publishing is all this body needs, so the slot names the framework's own capability and the
/// handler stays independent of the broker it is mounted on.
#[subscriber("devices/dev42/telemetry")]
async fn raise_alert(telemetry: &Telemetry, Out(out): Out<impl Publisher>) -> HandlerOutcome {
    if telemetry.temperature <= 30.0 {
        return HandlerOutcome::ack();
    }
    let alert = Alert {
        device: telemetry.device.clone(),
    };
    if out.message(&alert).publish().await.is_err() {
        return HandlerOutcome::retry();
    }
    HandlerOutcome::ack()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_handler_publishes_through_its_slot_on_the_in_process_broker() {
    let app = RustStream::new(AppInfo::new("mqtt-handlers", "0.0.0")).with_broker(
        MqttTestBroker::new(),
        |b| {
            b.include(raise_alert).publisher(MqttTestPublish);
        },
    );

    let tb = TestApp::start(app).await.expect("the harness starts");
    let reading = Telemetry {
        device: "dev42".to_owned(),
        temperature: 31.5,
    };
    tb.broker::<MqttTestBroker>()
        .message(&reading)
        .to(TELEMETRY)
        .publish()
        .await
        .expect("the injected reading is routed");

    tb.broker::<MqttTestBroker>()
        .subscriber(TELEMETRY)
        .assert_called_once()
        .with(&reading)
        .settled(HandlerOutcome::ack());
    tb.broker::<MqttTestBroker>()
        .published::<Alert>("alerts")
        .assert_called_once()
        .with(&Alert {
            device: "dev42".to_owned(),
        });
}

/// A device state is bytes on the wire rather than an encoded model, so the type carries its own
/// bytes and no codec runs on them.
#[derive(Outgoing, Serialized)]
#[outgoing(name = "devices/dev42/state")]
struct DeviceState(Vec<u8>);

/// A body that needs the two arguments MQTT carries on every PUBLISH packet bounds its slot with
/// this crate's own [`MqttPublishOptions`] instead, which the framework's slot wrapper carries
/// through to the paired publisher.
#[subscriber("devices/dev42/telemetry")]
async fn announce_state(
    telemetry: &Telemetry,
    Out(out): Out<impl MqttPublishOptions>,
) -> HandlerOutcome {
    let state = if telemetry.temperature > 30.0 {
        "hot"
    } else {
        "ok"
    };
    if out
        .with_retain(true)
        .with_qos(Qos::ExactlyOnce)
        .message(&DeviceState(state.as_bytes().to_vec()))
        .publish()
        .await
        .is_err()
    {
        return HandlerOutcome::retry();
    }
    HandlerOutcome::ack()
}

/// Quality of service and retain are transport behaviour the in-process broker deliberately does
/// not reproduce, so a body bound to them mounts on the real broker and the mount itself is what
/// is checked here; the wire behaviour belongs to the live suite. Building the app is I/O-free.
#[test]
fn a_slot_bound_with_the_crate_capability_mounts_on_the_broker() {
    let _app = RustStream::new(AppInfo::new("mqtt-handlers", "0.0.0")).with_broker(
        MqttBroker::new("mqtt://localhost:1883", "mqtt-handlers"),
        |b| {
            b.include(announce_state).publisher(MqttPublish::default());
        },
    );
}
