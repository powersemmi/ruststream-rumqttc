//! Handlers on this broker, driven through the framework's own surfaces rather than the broker
//! SPI: a `#[subscriber]` body runs on the production broker connected in process under
//! `TestApp`, the crate's per-message publish steps are reached through an injected `Out` slot, and
//! the crate's own descriptors and publish policy mount as a service writes them.
//!
//! The live suite (`integration_mqtt.rs`) covers the transport; this file covers the seam
//! between the crate and the framework's dispatch and injection paths, which needs no server.

#![cfg(feature = "testing")]

use std::time::Duration;

// The derive and the value a publish transform reads share the name in different namespaces: the
// prelude carries the macro `ruststream::Outgoing`, and this is the type `runtime::Outgoing`.
use ruststream::runtime::{Outgoing, PublishContext, RETRY_COUNT_HEADER};
use ruststream::testing::TestApp;
use ruststream_rumqttc::prelude::*;
use serde::{Deserialize, Serialize};

// The topic a device publishes to. The handlers below name it as a literal and assert on the same
// subscription; the one declared with `MqttFilter` covers it with a wildcard instead, so its
// assertions address the filter.
const TELEMETRY: &str = "devices/dev42/telemetry";

/// The broker a service builds its app on. `TestApp::start` connects it in process, so the address
/// is never dialled.
fn broker() -> MqttBroker {
    MqttBroker::new("mqtt://localhost:1883", "mqtt-handlers")
}

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
    let app = RustStream::new(AppInfo::new("mqtt-handlers", "0.0.0")).with_broker(broker(), |b| {
        b.include(raise_alert)
            .out(DefaultSlot, Publish::default())
            .build();
    });

    let tb = TestApp::start(app).await.expect("the harness starts");
    let reading = Telemetry {
        device: "dev42".to_owned(),
        temperature: 31.5,
    };
    tb.broker::<MqttBroker>()
        .message(&reading)
        .to(TELEMETRY)
        .publish()
        .await
        .expect("the injected reading is routed");

    tb.broker::<MqttBroker>()
        .subscriber(TELEMETRY)
        .assert_called_once()
        .with(&reading)
        .settled(HandlerOutcome::ack());
    tb.broker::<MqttBroker>()
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
    let app = RustStream::new(AppInfo::new("mqtt-handlers", "0.0.0")).with_broker(broker(), |b| {
        b.include(announce_state)
            .out(States, Publish::default())
            .build();
    });

    let tb = TestApp::start(app).await.expect("the harness starts");
    tb.broker::<MqttBroker>()
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
    let delivered = tb.broker::<MqttBroker>().published::<()>(STATE);
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
    let app = RustStream::new(AppInfo::new("mqtt-handlers", "0.0.0")).with_broker(broker(), |b| {
        b.include(announce_plainly)
            .out(States, Publish::default().qos(Qos::ExactlyOnce))
            .build();
    });

    let tb = TestApp::start(app).await.expect("the harness starts");
    tb.broker::<MqttBroker>()
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
    let app = RustStream::new(AppInfo::new("mqtt-handlers", "0.0.0")).with_broker(broker(), |b| {
        b.include(ingest.batch(nonzero!(1)));
    });

    let tb = TestApp::start(app).await.expect("the harness starts");
    for temperature in [21.5, 22.0, 23.5] {
        tb.broker::<MqttBroker>()
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

    let broker = tb.broker::<MqttBroker>();
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

const WILDCARD: &str = "devices/+/telemetry";

/// The declaration a service ships: the crate's own descriptor, with the wildcard, the quality of
/// service and the shared group a fleet subscription carries. Nothing about it is written for a
/// test, and the tests below mount this one handle on both brokers.
#[subscriber(MqttFilter::new("devices/+/telemetry").qos(Qos::AtLeastOnce).shared("workers"))]
async fn collect_telemetry(telemetry: &Telemetry) -> HandlerOutcome {
    let _ = telemetry.temperature;
    HandlerOutcome::ack()
}

/// The wildcard resolves in process the way it resolves on the wire, so the message a device
/// would publish reaches the body under its own topic - not under the filter, which is not a
/// topic a producer could publish to at all.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_declaration_a_service_ships_mounts_on_the_in_process_broker() {
    let app = RustStream::new(AppInfo::new("mqtt-handlers", "0.0.0")).with_broker(broker(), |b| {
        b.include(collect_telemetry)
            .out_retry(Publish::default())
            .to(TELEMETRY);
    });

    let tb = TestApp::start(app).await.expect("the harness starts");
    let reading = Telemetry {
        device: "dev42".to_owned(),
        temperature: 21.5,
    };
    tb.broker::<MqttBroker>()
        .message(&reading)
        .to(TELEMETRY)
        .publish()
        .await
        .expect("the injected reading is routed");

    tb.broker::<MqttBroker>()
        .subscriber(WILDCARD)
        .assert_called_once()
        .with(&reading)
        .settled(HandlerOutcome::ack());
}

/// A filter with `#` anywhere but last is one no broker would accept.
#[subscriber(MqttFilter::new("devices/#/telemetry"))]
async fn never_subscribes(telemetry: &Telemetry) -> HandlerOutcome {
    let _ = telemetry.temperature;
    HandlerOutcome::ack()
}

/// The descriptor is validated on this transport too, so a filter a server would reject fails
/// the same way here instead of passing its first test in process and its first deployment
/// nowhere.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_descriptor_a_server_would_reject_does_not_start_here_either() {
    let app = RustStream::new(AppInfo::new("mqtt-handlers", "0.0.0")).with_broker(broker(), |b| {
        b.include(never_subscribes)
            .out_retry(Publish::default())
            .to(TELEMETRY);
    });

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
#[subscriber(MqttFilter::new("devices/+/ping").qos(Qos::AtLeastOnce), publish("devices/dev42/pong"))]
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
    let app = RustStream::new(AppInfo::new("mqtt-handlers", "0.0.0")).with_broker(broker(), |b| {
        b.include(answer_ping)
            .out_reply(Publish::default())
            .out_retry(Publish::default())
            .to(PING);
    });

    let tb = TestApp::start(app).await.expect("the harness starts");
    tb.broker::<MqttBroker>()
        .message(&Telemetry {
            device: "dev42".to_owned(),
            temperature: 21.5,
        })
        .to(PING)
        .publish()
        .await
        .expect("the injected ping is routed");

    tb.broker::<MqttBroker>()
        .published::<Pong>(PONG)
        .assert_called_once()
        .with(&Pong {
            device: "dev42".to_owned(),
        });
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
    let app = RustStream::new(AppInfo::new("mqtt-handlers", "0.0.0")).with_broker(broker(), |b| {
        b.include(acknowledge);
    });

    let tb = TestApp::start(app).await.expect("the harness starts");
    let command = Command { id: 7 };
    tb.broker::<MqttBroker>()
        .message(&command)
        .to(COMMANDS)
        .publish()
        .await
        .expect("the injected command is routed");

    tb.broker::<MqttBroker>()
        .subscriber(COMMANDS)
        .assert_called_once()
        .with(&command);
    tb.broker::<MqttBroker>()
        .published::<Ack>(ACKS)
        .assert_called_once()
        .with(&Ack { id: 7 });
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_reply_that_declares_no_topic_lands_on_the_mount_site_one() {
    let app = RustStream::new(AppInfo::new("mqtt-handlers", "0.0.0")).with_broker(broker(), |b| {
        b.include(issue_receipt).out_reply(Publish::default());
    });

    let tb = TestApp::start(app).await.expect("the harness starts");
    let command = Command { id: 11 };
    tb.broker::<MqttBroker>()
        .message(&command)
        .to(AUDITED)
        .publish()
        .await
        .expect("the injected command is routed");

    tb.broker::<MqttBroker>()
        .subscriber(AUDITED)
        .assert_called_once()
        .with(&command);
    tb.broker::<MqttBroker>()
        .published::<Receipt>(RECEIPTS)
        .assert_called_once()
        .with(&Receipt { id: 11 });
}

const DEFERRED: &str = "devices/dev42/deferred";
const DEFERRED_WILDCARD: &str = "devices/+/deferred";

/// Long enough that no other timer in the test is due at the same instant; the clock is paused,
/// so nothing waits for it.
const RETRY_DELAY: Duration = Duration::from_secs(5);

/// Stamps every deferred copy with the subscription the delivery came from, which is what marks
/// a redelivery as one downstream. A transform on the retry position reads the delivery being
/// retried, the way a reply's does. It sets neither of the two arguments MQTT carries, so it is
/// generic over the options a position writes and mounts over any publisher.
#[derive(Debug, Clone, Copy)]
struct StampRetry;

impl<C, Options> PublishTransform<ForReply<C>, Options> for StampRetry {
    type Destination = Reads;

    fn apply(
        &self,
        out: &mut Outgoing<'_>,
        _options: &mut Option<Options>,
        cx: &PublishContext<'_, C>,
    ) {
        out.headers_mut()
            .insert("x-retried-from", cx.name().to_owned());
    }
}

/// Defers the first delivery and settles the copy, so one subscription runs both legs of the
/// fallback.
#[subscriber("devices/dev42/deferred")]
async fn reconcile(telemetry: &Telemetry, ctx: &mut Context) -> HandlerOutcome {
    let _ = telemetry.temperature;
    let attempt = ctx
        .headers()
        .get_str(RETRY_COUNT_HEADER)
        .and_then(|count| count.parse::<u64>().ok())
        .unwrap_or(0);
    if attempt == 0 {
        HandlerOutcome::retry_after(RETRY_DELAY)
    } else {
        HandlerOutcome::ack()
    }
}

/// MQTT cannot defer a redelivery, so `retry_after` runs on the framework's fallback: the
/// original is dropped and a copy is published back to the topic the subscription reported. The
/// copy leaves through the publisher the registration bound at the mount site, transforms
/// included, which is the only place a service can mark it.
#[tokio::test(start_paused = true)]
async fn a_transform_on_the_retry_position_stamps_the_deferred_copy() {
    let app = RustStream::new(AppInfo::new("mqtt-handlers", "0.0.0")).with_broker(broker(), |b| {
        b.include(reconcile)
            .out_retry(Publish::default())
            .transform(StampRetry);
    });

    let tb = TestApp::start(app).await.expect("the harness starts");
    tb.broker::<MqttBroker>()
        .message(&Telemetry {
            device: "dev42".to_owned(),
            temperature: 21.5,
        })
        .to(DEFERRED)
        .publish()
        .await
        .expect("the injected reading is routed");
    tb.advance(RETRY_DELAY)
        .await
        .expect("the deferred copy is published and handled");

    tb.broker::<MqttBroker>()
        .published::<Telemetry>(DEFERRED)
        .with_header("x-retried-from", DEFERRED);
    tb.broker::<MqttBroker>()
        .subscriber(DEFERRED)
        .assert_called(2)
        .settled(HandlerOutcome::ack());
}

/// A filter subscription reads every topic it matches and addresses none of them, so the mount
/// site says where a copy goes. This one settles the copy, so both legs run on one subscription.
#[subscriber(MqttFilter::new("devices/+/deferred"))]
async fn reconcile_anywhere(telemetry: &Telemetry, ctx: &mut Context) -> HandlerOutcome {
    let _ = telemetry.temperature;
    let attempt = ctx
        .headers()
        .get_str(RETRY_COUNT_HEADER)
        .and_then(|count| count.parse::<u64>().ok())
        .unwrap_or(0);
    if attempt == 0 {
        HandlerOutcome::retry_after(RETRY_DELAY)
    } else {
        HandlerOutcome::ack()
    }
}

/// A filter names no destination of its own, so a registration on one that names none either
/// refuses to start. The service learns at startup that `retry_after` has nowhere to go, rather
/// than losing every delayed message to a publish that reaches nothing.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_filter_registration_that_names_no_destination_does_not_start() {
    let app = RustStream::new(AppInfo::new("mqtt-handlers", "0.0.0")).with_broker(broker(), |b| {
        b.include(reconcile_anywhere);
    });

    let error = TestApp::start(app)
        .await
        .expect_err("a filter subscription cannot name where its redelivery is published");
    let reported = error.to_string();
    assert!(
        reported.contains(DEFERRED_WILDCARD),
        "the startup error names the subscription that addresses nothing: {reported}"
    );
    assert!(
        reported.contains("MqttFilter"),
        "the startup error names the descriptor that addresses nothing: {reported}"
    );
}

/// The same registration with the destination named: a topic the filter matches, so the copy
/// comes back to this subscription and the handler settles it on the second delivery.
#[tokio::test(start_paused = true)]
async fn a_filter_registration_publishes_its_copies_where_the_mount_site_names() {
    let app = RustStream::new(AppInfo::new("mqtt-handlers", "0.0.0")).with_broker(broker(), |b| {
        b.include(reconcile_anywhere)
            .out_retry(Publish::default())
            .to(DEFERRED);
    });

    let tb = TestApp::start(app).await.expect("the harness starts");
    tb.broker::<MqttBroker>()
        .message(&Telemetry {
            device: "dev42".to_owned(),
            temperature: 21.5,
        })
        .to(DEFERRED)
        .publish()
        .await
        .expect("the injected reading is routed");
    tb.advance(RETRY_DELAY)
        .await
        .expect("the deferred copy is published and handled");

    tb.broker::<MqttBroker>()
        .published::<Telemetry>(DEFERRED)
        .with_header(RETRY_COUNT_HEADER, "1");
    tb.broker::<MqttBroker>()
        .subscriber(DEFERRED_WILDCARD)
        .assert_called(2)
        .settled(HandlerOutcome::ack());
}

const CAPPED: &str = "devices/dev42/capped";
const CAPPED_WILDCARD: &str = "devices/+/capped";
/// Outside the filter's match set on purpose: a dead-letter topic the subscription itself reads
/// hands the spent delivery straight back.
const DEAD: &str = "dead/capped";

/// A handler that never settles, which is what the cap is for.
#[subscriber(MqttTopic::new("devices/dev42/capped"))]
async fn never_settles(telemetry: &Telemetry) -> HandlerOutcome {
    let _ = telemetry.temperature;
    HandlerOutcome::retry_after(RETRY_DELAY)
}

/// The same body on a filter subscription, where the mount site also names where the copies go.
#[subscriber(MqttFilter::new("devices/+/capped"))]
async fn never_settles_anywhere(telemetry: &Telemetry) -> HandlerOutcome {
    let _ = telemetry.temperature;
    HandlerOutcome::retry_after(RETRY_DELAY)
}

/// MQTT counts no redeliveries of its own, so the cap is counted on the framework's retry-count
/// header and the copies the runtime publishes carry it. A topic subscription addresses its own
/// copies, so the declaration is the whole mount site.
#[tokio::test(start_paused = true)]
async fn a_capped_registration_on_a_topic_dead_letters_the_spent_delivery() {
    let app = RustStream::new(AppInfo::new("mqtt-handlers", "0.0.0")).with_broker(broker(), |b| {
        b.include(never_settles)
            .max_attempts(nonzero!(2u32))
            .dead_letter(DEAD);
    });

    let tb = TestApp::start(app).await.expect("the harness starts");
    tb.broker::<MqttBroker>()
        .message(&Telemetry {
            device: "dev42".to_owned(),
            temperature: 21.5,
        })
        .to(CAPPED)
        .publish()
        .await
        .expect("the injected reading is routed");
    tb.advance(RETRY_DELAY)
        .await
        .expect("the first copy is published and handled");
    tb.advance(RETRY_DELAY)
        .await
        .expect("the spent delivery leaves for the dead-letter topic");

    tb.broker::<MqttBroker>()
        .subscriber(CAPPED)
        .assert_called(2);
    tb.broker::<MqttBroker>()
        .published::<Telemetry>(DEAD)
        .assert_called_once();
}

/// The same declaration over a filter subscription: the cap and the destination read the same,
/// and what the filter adds is the topic its copies are published to.
#[tokio::test(start_paused = true)]
async fn a_capped_registration_on_a_filter_dead_letters_the_spent_delivery() {
    let app = RustStream::new(AppInfo::new("mqtt-handlers", "0.0.0")).with_broker(broker(), |b| {
        b.include(never_settles_anywhere)
            .max_attempts(nonzero!(2u32))
            .dead_letter(DEAD)
            .out_retry(Publish::default())
            .to(CAPPED);
    });

    let tb = TestApp::start(app).await.expect("the harness starts");
    tb.broker::<MqttBroker>()
        .message(&Telemetry {
            device: "dev42".to_owned(),
            temperature: 21.5,
        })
        .to(CAPPED)
        .publish()
        .await
        .expect("the injected reading is routed");
    tb.advance(RETRY_DELAY)
        .await
        .expect("the first copy is published and handled");
    tb.advance(RETRY_DELAY)
        .await
        .expect("the spent delivery leaves for the dead-letter topic");

    tb.broker::<MqttBroker>()
        .subscriber(CAPPED_WILDCARD)
        .assert_called(2);
    tb.broker::<MqttBroker>()
        .published::<Telemetry>(DEAD)
        .assert_called_once();
}

const FLEET: &str = "devices/+/deferred";
const DEVICE_42: &str = "devices/42/deferred";
const DEVICE_43: &str = "devices/43/deferred";

/// Sends every copy back to the topic its delivery arrived on, which is what a filter
/// subscription needs: its many topics have no single answer, and each message belongs to one.
struct ToDeliveryTopic;

impl<Options> PublishTransform<ForReply<MqttContext>, Options> for ToDeliveryTopic {
    type Destination = Names;

    fn apply(
        &self,
        out: &mut Outgoing<'_>,
        _options: &mut Option<Options>,
        cx: &PublishContext<'_, MqttContext>,
    ) {
        out.set_name(cx.context(DeliveryTopic).to_owned());
    }
}

/// Defers the first delivery and settles the copy, reading the topic it arrived on so the
/// registration's context type is the crate's.
#[subscriber(MqttFilter::new("devices/+/deferred"))]
async fn reconcile_per_device(
    telemetry: &Telemetry,
    ctx: &mut Context<'_, MqttContext>,
) -> HandlerOutcome {
    let _ = telemetry.temperature;
    let attempt = ctx
        .headers()
        .get_str(RETRY_COUNT_HEADER)
        .and_then(|count| count.parse::<u64>().ok())
        .unwrap_or(0);
    if attempt == 0 {
        HandlerOutcome::retry_after(RETRY_DELAY)
    } else {
        HandlerOutcome::ack()
    }
}

/// A filter reads many topics, so naming one at the mount site would send every copy to the same
/// device. The transform names the destination per delivery instead, and the copy of a message
/// published to one device's topic comes back on that device's topic.
#[tokio::test(start_paused = true)]
async fn a_naming_transform_returns_a_copy_to_the_topic_it_arrived_on() {
    let app = RustStream::new(AppInfo::new("mqtt-handlers", "0.0.0")).with_broker(broker(), |b| {
        b.include(reconcile_per_device)
            .out_retry(Publish::default())
            .transform(ToDeliveryTopic);
    });

    let tb = TestApp::start(app).await.expect("the harness starts");
    tb.broker::<MqttBroker>()
        .message(&Telemetry {
            device: "42".to_owned(),
            temperature: 21.5,
        })
        .to(DEVICE_42)
        .publish()
        .await
        .expect("the injected reading is routed");
    tb.advance(RETRY_DELAY)
        .await
        .expect("the deferred copy is published and handled");

    tb.broker::<MqttBroker>()
        .published::<Telemetry>(DEVICE_42)
        .with_header(RETRY_COUNT_HEADER, "1");
    tb.broker::<MqttBroker>()
        .published::<Telemetry>(DEVICE_43)
        .assert_not_called();
    tb.broker::<MqttBroker>()
        .published::<Telemetry>(FLEET)
        .assert_not_called();
    tb.broker::<MqttBroker>()
        .subscriber(FLEET)
        .assert_called(2)
        .settled(HandlerOutcome::ack());
}

const READING: &str = "sensors/s1/reading";

#[subscriber(MqttFilter::new("sensors/+/reading"))]
async fn record_reading(telemetry: &Telemetry) {
    let _ = telemetry.temperature;
}

#[subscriber(MqttFilter::new("sensors/#"))]
async fn archive_sensor_traffic(telemetry: &Telemetry) {
    let _ = telemetry.temperature;
}

/// Two handlers whose filters both match a topic each run once for a message published there.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn overlapping_filters_run_each_handler_once() {
    let app = RustStream::new(AppInfo::new("mqtt-handlers", "0.0.0")).with_broker(broker(), |b| {
        // A filter is no topic to send a deferred copy to, so each mount names one.
        b.include(record_reading)
            .out_retry(Publish::default())
            .to(READING);
        b.include(archive_sensor_traffic)
            .out_retry(Publish::default())
            .to(READING);
    });

    let tb = TestApp::start(app).await.expect("the harness starts");
    let reading = Telemetry {
        device: "s1".to_owned(),
        temperature: 19.0,
    };
    tb.broker::<MqttBroker>()
        .message(&reading)
        .to(READING)
        .publish()
        .await
        .expect("the injected reading is routed");

    for filter in ["sensors/+/reading", "sensors/#"] {
        tb.broker::<MqttBroker>()
            .subscriber(filter)
            .assert_called_once()
            .with(&reading)
            .settled(HandlerOutcome::ack());
    }
}
