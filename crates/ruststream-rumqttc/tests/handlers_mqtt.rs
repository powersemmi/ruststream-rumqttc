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
use ruststream_rumqttc::{QOS_HEADER, RETAIN_HEADER};
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
            b.include(raise_alert)
                .out(DefaultSlot, MqttTestPublish)
                .build();
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

const STATE: &str = "devices/dev42/state";

/// A device state is bytes on the wire rather than an encoded model, so the type carries its own
/// bytes and no codec runs on them.
#[derive(Outgoing, Serialized)]
#[outgoing(name = "devices/dev42/state")]
struct DeviceState(Vec<u8>);

#[derive(OutSlot)]
#[publishes(DeviceState)]
struct States;

/// A body that needs the two arguments MQTT carries on every PUBLISH packet bounds its slot with
/// this crate's own [`MqttPublishOptions`] instead. The step resolves on the slot entry the body
/// holds, so the publish stays the slot's; resolving one layer down would reach the same wire and
/// lose the attribution.
#[subscriber("devices/dev42/telemetry")]
async fn announce_state(
    telemetry: &Telemetry,
    Out(out): Out<impl MqttPublishOptions, States>,
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

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_per_message_arguments_ride_the_slot_and_stop_at_the_transport() {
    let app = RustStream::new(AppInfo::new("mqtt-handlers", "0.0.0")).with_broker(
        MqttTestBroker::new(),
        |b| {
            b.include(announce_state)
                .out(States, MqttTestPublish)
                .build();
        },
    );

    let tb = TestApp::start(app).await.expect("the harness starts");
    tb.broker::<MqttTestBroker>()
        .message(&Telemetry {
            device: "dev42".to_owned(),
            temperature: 31.5,
        })
        .to(TELEMETRY)
        .publish()
        .await
        .expect("the injected reading is routed");

    // The slot saw it, which is what says the step resolved on the entry rather than past it.
    let states = tb.out::<States>().assert_called_once().with_raw(b"hot");
    let attributed = &states.messages()[0];
    assert_eq!(attributed.name(), STATE);
    assert_eq!(
        attributed.headers().get_str(QOS_HEADER),
        Some("2"),
        "the arguments travel with the message to the publisher"
    );
    assert_eq!(attributed.headers().get_str(RETAIN_HEADER), Some("true"));

    // The transport consumed them, so a subscriber sees a plain message.
    let delivered = tb.broker::<MqttTestBroker>().published::<()>(STATE);
    let delivered = delivered.assert_called_once().messages()[0]
        .headers()
        .clone();
    assert_eq!(delivered.get(QOS_HEADER), None);
    assert_eq!(delivered.get(RETAIN_HEADER), None);
}

/// The same body mounts on the real broker, which is where the two arguments reach a wire.
/// Building the app is I/O-free, so the mount is what this checks; the wire effect is the live
/// suite's.
#[test]
fn a_slot_bound_with_the_crate_capability_mounts_on_the_real_broker() {
    let _app = RustStream::new(AppInfo::new("mqtt-handlers", "0.0.0")).with_broker(
        MqttBroker::new("mqtt://localhost:1883", "mqtt-handlers"),
        |b| {
            b.include(announce_state)
                .out(States, Publish::default())
                .build();
        },
    );
}

const READINGS: &str = "devices/dev42/readings";

/// A page handler: MQTT delivers one PUBLISH packet at a time, so the pages are assembled on the
/// client, and nothing in this body or its mount site says so.
#[subscriber("devices/dev42/readings")]
async fn ingest(readings: &[Telemetry]) -> HandlerOutcome {
    let _ = readings.len();
    HandlerOutcome::ack()
}

/// The size a mount site names is the size the pages come back at. One is the split that holds
/// without a replayable log to publish into ahead of the subscription; the conformance batch
/// suite covers the general case at size three, against this transport and a server alike.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_page_handler_is_handed_pages_of_the_size_its_mount_site_named() {
    let app = RustStream::new(AppInfo::new("mqtt-handlers", "0.0.0")).with_broker(
        MqttTestBroker::new(),
        |b| {
            b.include(ingest.batch(nonzero!(1)));
        },
    );

    let tb = TestApp::start(app).await.expect("the harness starts");
    for temperature in [21.5, 22.0, 23.5] {
        tb.broker::<MqttTestBroker>()
            .message(&Telemetry {
                device: "dev42".to_owned(),
                temperature,
            })
            .to(READINGS)
            .publish()
            .await
            .expect("the injected reading is routed");
    }
    tb.settle().await.expect("the pages settle");

    let broker = tb.broker::<MqttTestBroker>();
    let subscriber = broker.subscriber(READINGS);
    assert_eq!(
        subscriber.received::<Telemetry>().len(),
        3,
        "every reading reaches the body"
    );
    subscriber
        .assert_page_sizes(&[1, 1, 1])
        .settled(HandlerOutcome::ack());
}

/// The page handler mounts on the real broker too: its subscriber carries the same capability,
/// which is the whole of what a `&[T]` body asks of a transport.
#[test]
fn a_page_handler_mounts_on_the_real_broker() {
    let _app = RustStream::new(AppInfo::new("mqtt-handlers", "0.0.0")).with_broker(
        MqttBroker::new("mqtt://localhost:1883", "mqtt-handlers"),
        |b| {
            b.include(ingest.batch(nonzero!(8)));
        },
    );
}
