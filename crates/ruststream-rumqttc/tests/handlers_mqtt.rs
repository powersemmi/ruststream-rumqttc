//! Handlers on this broker, driven through the framework's own surfaces rather than the broker
//! SPI: a `#[subscriber]` body runs on the in-process transport under `TestApp`, the crate's
//! per-message publish steps are reached through an injected `Out` slot, and the crate's own
//! descriptor and publish policy mount on both brokers from one routes file.
//!
//! The live suite (`integration_mqtt.rs`) covers the transport; this file covers the seam
//! between the crate and the framework's dispatch and injection paths, which needs no server.

#![cfg(feature = "testing")]

use ruststream::testing::TestApp;
use ruststream_rumqttc::prelude::*;
use ruststream_rumqttc::testing::MqttTestBroker;
use serde::{Deserialize, Serialize};

// The topic a device publishes to. The handlers below name it as a literal and assert on the same
// subscription; the one declared with `MqttTopic` covers it with a wildcard instead, so its
// assertions address the filter.
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
                .out(DefaultSlot, Publish::default())
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
const HEARTBEAT: &str = "devices/dev42/heartbeat";

/// A device state is bytes on the wire rather than an encoded model, so the type carries its own
/// bytes and no codec runs on them.
#[derive(Outgoing, Serialized)]
#[outgoing(name = "devices/dev42/state")]
struct DeviceState(Vec<u8>);

#[derive(OutSlot)]
#[publishes(DeviceState)]
struct States;

/// A body that adjusts the two arguments MQTT carries on every PUBLISH packet names this crate's
/// options type in its slot bound. The steps sit on the publish builder, so the publish is still
/// the slot's own - attributed to `States`, and encoded with the codec the include site named.
#[subscriber("devices/dev42/telemetry")]
async fn announce_state(
    telemetry: &Telemetry,
    Out(out): Out<impl Publisher<Options = MqttPublishOptions>, States>,
) -> HandlerOutcome {
    let state = if telemetry.temperature > 30.0 {
        "hot"
    } else {
        "ok"
    };
    if out
        .message(&DeviceState(state.as_bytes().to_vec()))
        .retain(true)
        .qos(Qos::ExactlyOnce)
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
                .out(States, Publish::default())
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

    // The slot saw the publish and the arguments it carried, which is what says the steps
    // resolved on the slot's own entry rather than past it.
    let states = tb
        .out::<States>()
        .assert_called_once()
        .with_raw(b"hot")
        .with_options(
            &MqttPublishOptions::default()
                .retain(true)
                .qos(Qos::ExactlyOnce),
        );
    assert_eq!(states.messages()[0].name(), STATE);

    // They are protocol fields, so the publisher hands them to the client rather than to the
    // message: a subscriber sees a plain delivery.
    let delivered = tb.broker::<MqttTestBroker>().published::<()>(STATE);
    assert!(
        delivered.assert_called_once().messages()[0]
            .headers()
            .is_empty(),
        "nothing about the arguments reaches a subscriber as a header"
    );
}

/// The mirror case: a body that takes no step publishes entirely under the policy the mount site
/// named, and the slot view says so rather than reporting an empty options value.
#[subscriber("devices/dev42/heartbeat")]
async fn announce_plainly(
    telemetry: &Telemetry,
    Out(out): Out<impl Publisher<Options = MqttPublishOptions>, States>,
) -> HandlerOutcome {
    let _ = telemetry.temperature;
    if out
        .message(&DeviceState(b"alive".to_vec()))
        .publish()
        .await
        .is_err()
    {
        return HandlerOutcome::retry();
    }
    HandlerOutcome::ack()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_publish_that_takes_no_step_carries_the_policy_alone() {
    let app = RustStream::new(AppInfo::new("mqtt-handlers", "0.0.0")).with_broker(
        MqttTestBroker::new(),
        |b| {
            b.include(announce_plainly)
                .out(States, Publish::default().qos(Qos::ExactlyOnce))
                .build();
        },
    );

    let tb = TestApp::start(app).await.expect("the harness starts");
    tb.broker::<MqttTestBroker>()
        .message(&Telemetry {
            device: "dev42".to_owned(),
            temperature: 21.5,
        })
        .to(HEARTBEAT)
        .publish()
        .await
        .expect("the injected reading is routed");

    tb.out::<States>()
        .assert_called_once()
        .with_raw(b"alive")
        .assert_options_default();
}

/// The same body, and the same policy attached the same way, mount on the real broker - which is
/// where the two arguments reach a wire. Building the app is I/O-free, so the mount is what this
/// checks; the wire effect is the live suite's.
#[test]
fn a_slot_bound_with_the_crate_options_mounts_on_the_real_broker() {
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

/// A batch handler: MQTT delivers one PUBLISH packet at a time, so the batches are assembled on
/// the client, and nothing in this body or its mount site says so.
#[subscriber("devices/dev42/readings")]
async fn ingest(readings: &[Telemetry]) -> HandlerOutcome {
    let _ = readings.len();
    HandlerOutcome::ack()
}

/// The size a mount site names is the size the batches come back at. One is the split that holds
/// without a replayable log to publish into ahead of the subscription; the conformance batch
/// suite covers the general case at size three, against this transport and a server alike.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_batch_handler_is_handed_batches_of_the_size_its_mount_site_named() {
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
    tb.settle().await.expect("the batches settle");

    let broker = tb.broker::<MqttTestBroker>();
    let subscriber = broker.subscriber(READINGS);
    assert_eq!(
        subscriber.received::<Telemetry>().len(),
        3,
        "every reading reaches the body"
    );
    subscriber
        .assert_batch_sizes(&[1, 1, 1])
        .settled(HandlerOutcome::ack());
}

/// The batch handler mounts on the real broker too: its subscriber carries the same capability,
/// which is the whole of what a `&[T]` body asks of a transport.
#[test]
fn a_batch_handler_mounts_on_the_real_broker() {
    let _app = RustStream::new(AppInfo::new("mqtt-handlers", "0.0.0")).with_broker(
        MqttBroker::new("mqtt://localhost:1883", "mqtt-handlers"),
        |b| {
            b.include(ingest.batch(nonzero!(8)));
        },
    );
}

const WILDCARD: &str = "devices/+/telemetry";

/// The declaration a service ships: the crate's own descriptor, with the wildcard, the quality of
/// service and the shared group a fleet subscription carries. Nothing about it is written for a
/// test, and the tests below mount this one handle on both brokers.
#[subscriber(MqttTopic::new("devices/+/telemetry").qos(Qos::AtLeastOnce).shared("workers"))]
async fn collect_telemetry(telemetry: &Telemetry) -> HandlerOutcome {
    let _ = telemetry.temperature;
    HandlerOutcome::ack()
}

/// The wildcard resolves in process the way it resolves on the wire, so the message a device
/// would publish reaches the body under its own topic - not under the filter, which is not a
/// topic a producer could publish to at all.
///
/// What the descriptor asks for beyond the filter is the transport's, and this transport has
/// none: the `QoS` is not handshaked and the group is not distributed, so this settles what
/// routing did and says nothing about either.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_declaration_a_service_ships_mounts_on_the_in_process_broker() {
    let app = RustStream::new(AppInfo::new("mqtt-handlers", "0.0.0")).with_broker(
        MqttTestBroker::new(),
        |b| {
            b.include(collect_telemetry);
        },
    );

    let tb = TestApp::start(app).await.expect("the harness starts");
    let reading = Telemetry {
        device: "dev42".to_owned(),
        temperature: 21.5,
    };
    tb.broker::<MqttTestBroker>()
        .message(&reading)
        .to(TELEMETRY)
        .publish()
        .await
        .expect("the injected reading is routed");

    tb.broker::<MqttTestBroker>()
        .subscriber(WILDCARD)
        .assert_called_once()
        .with(&reading)
        .settled(HandlerOutcome::ack());
}

/// The same handle, the same descriptor, the other broker. Building the app is I/O-free, so the
/// mount is what this checks, and it is the whole claim: one declaration serves production and
/// the harness alike.
#[test]
fn the_same_declaration_mounts_on_the_real_broker() {
    let _app = RustStream::new(AppInfo::new("mqtt-handlers", "0.0.0")).with_broker(
        MqttBroker::new("mqtt://localhost:1883", "mqtt-handlers"),
        |b| {
            b.include(collect_telemetry);
        },
    );
}

/// A filter with `#` anywhere but last is one no broker would accept.
#[subscriber(MqttTopic::new("devices/#/telemetry"))]
async fn never_subscribes(telemetry: &Telemetry) -> HandlerOutcome {
    let _ = telemetry.temperature;
    HandlerOutcome::ack()
}

/// The descriptor is validated on this transport too, so a filter a server would reject fails
/// the same way here instead of passing its first test in process and its first deployment
/// nowhere.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_descriptor_a_server_would_reject_does_not_start_here_either() {
    let app = RustStream::new(AppInfo::new("mqtt-handlers", "0.0.0")).with_broker(
        MqttTestBroker::new(),
        |b| {
            b.include(never_subscribes);
        },
    );

    let error = TestApp::start(app)
        .await
        .expect_err("an invalid topic filter cannot open a subscription");
    assert!(
        error.to_string().contains("devices/#/telemetry"),
        "the startup error names the filter it refused, not just the subscription: {error}"
    );
}

const PING: &str = "devices/dev42/ping";
const PONG: &str = "devices/dev42/pong";

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
struct Pong {
    device: String,
}

/// A responder in the shape a service writes it: the subscription is the crate's descriptor and
/// the attribute names where the answer goes, so the body returns the reply instead of publishing
/// it by hand.
#[subscriber(MqttTopic::new("devices/+/ping").qos(Qos::AtLeastOnce), publish("devices/dev42/pong"))]
async fn answer_ping(ping: &Telemetry) -> Pong {
    Pong {
        device: ping.device.clone(),
    }
}

/// The whole routes line, both halves of it, on the in-process broker: the descriptor names the
/// subscription and the production policy is attached to the reply slot. This is the line a
/// service ships, character for character, and the reply comes out where the attribute said.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_production_routes_line_mounts_whole_on_the_in_process_broker() {
    let app = RustStream::new(AppInfo::new("mqtt-handlers", "0.0.0")).with_broker(
        MqttTestBroker::new(),
        |b| {
            b.include(answer_ping).out(Reply, Publish::default());
        },
    );

    let tb = TestApp::start(app).await.expect("the harness starts");
    tb.broker::<MqttTestBroker>()
        .message(&Telemetry {
            device: "dev42".to_owned(),
            temperature: 21.5,
        })
        .to(PING)
        .publish()
        .await
        .expect("the injected ping is routed");

    tb.broker::<MqttTestBroker>()
        .published::<Pong>(PONG)
        .assert_called_once()
        .with(&Pong {
            device: "dev42".to_owned(),
        });
}

/// The same routes line on the real broker. The policy is the one that reaches a wire there, and
/// nothing at the mount site had to change to get here.
#[test]
fn a_production_routes_line_mounts_on_the_real_broker() {
    let _app = RustStream::new(AppInfo::new("mqtt-handlers", "0.0.0")).with_broker(
        MqttBroker::new("mqtt://localhost:1883", "mqtt-handlers"),
        |b| {
            b.include(answer_ping).out(Reply, Publish::default());
        },
    );
}

const COMMANDS: &str = "devices/dev42/commands";
const ACKS: &str = "devices/dev42/acks";
const AUDITED: &str = "devices/dev42/audited";
const RECEIPTS: &str = "devices/dev42/receipts";

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
struct Command {
    id: u64,
}

/// A device acknowledgement is answered on one topic wherever the handler is mounted, so the type
/// states it.
#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
#[outgoing(name = "devices/dev42/acks")]
struct Ack {
    id: u64,
}

#[subscriber("devices/dev42/commands", publish)]
async fn acknowledge(command: &Command) -> Ack {
    Ack { id: command.id }
}

/// An audit receipt goes wherever the deployment collects them, so the type leaves the topic to
/// the mount site.
#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
struct Receipt {
    id: u64,
}

#[subscriber("devices/dev42/audited", publish("devices/dev42/receipts"))]
async fn issue_receipt(command: &Command) -> Receipt {
    Receipt { id: command.id }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_reply_lands_on_the_topic_its_type_declares() {
    let app = RustStream::new(AppInfo::new("mqtt-handlers", "0.0.0")).with_broker(
        MqttTestBroker::new(),
        |b| {
            b.include(acknowledge);
        },
    );

    let tb = TestApp::start(app).await.expect("the harness starts");
    let command = Command { id: 7 };
    tb.broker::<MqttTestBroker>()
        .message(&command)
        .to(COMMANDS)
        .publish()
        .await
        .expect("the injected command is routed");

    tb.broker::<MqttTestBroker>()
        .subscriber(COMMANDS)
        .assert_called_once()
        .with(&command);
    tb.broker::<MqttTestBroker>()
        .published::<Ack>(ACKS)
        .assert_called_once()
        .with(&Ack { id: 7 });
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_reply_that_declares_no_topic_lands_on_the_mount_site_one() {
    let app = RustStream::new(AppInfo::new("mqtt-handlers", "0.0.0")).with_broker(
        MqttTestBroker::new(),
        |b| {
            b.include(issue_receipt).out(Reply, Publish::default());
        },
    );

    let tb = TestApp::start(app).await.expect("the harness starts");
    let command = Command { id: 11 };
    tb.broker::<MqttTestBroker>()
        .message(&command)
        .to(AUDITED)
        .publish()
        .await
        .expect("the injected command is routed");

    tb.broker::<MqttTestBroker>()
        .subscriber(AUDITED)
        .assert_called_once()
        .with(&command);
    tb.broker::<MqttTestBroker>()
        .published::<Receipt>(RECEIPTS)
        .assert_called_once()
        .with(&Receipt { id: 11 });
}

/// A reply leaves through an ordinary publisher, so the two arguments MQTT carries on every
/// PUBLISH packet stay the reply policy's: the topic the type declares says where the packet goes
/// and nothing about how it is sent. The wire effect is the live suite's; the mount is what this
/// checks.
#[test]
fn a_reply_on_a_declared_topic_takes_the_arguments_of_its_policy() {
    let _app = RustStream::new(AppInfo::new("mqtt-handlers", "0.0.0")).with_broker(
        MqttBroker::new("mqtt://localhost:1883", "mqtt-handlers"),
        |b| {
            b.include(acknowledge)
                .out(Reply, Publish::default().qos(Qos::ExactlyOnce).retain(true));
        },
    );
}
