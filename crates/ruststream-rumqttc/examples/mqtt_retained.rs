//! Retained messages: the broker keeps the last message per topic and hands it to new
//! subscribers.
//!
//! Retain is declared for a whole publisher on the publish policy, or for one message on the
//! publish. A startup announcement goes through the scope's `after_startup` hook, which runs once
//! the broker is connected, so it cannot race the connection; a handler announces from its own
//! slot.
//!
//! Run a broker first (`just brokers-up`), then:
//! `cargo run --example mqtt_retained -- run`

use ruststream::runtime::PublishError;
// The error type is named here because this example handles it, in the hooks' return types.
use ruststream_rumqttc::MqttError;
use ruststream_rumqttc::prelude::*;
use serde::Deserialize;

// --8<-- [start:state]
/// A device state announcement. An MQTT state is bytes on the wire rather than an encoded
/// model, so the type carries its own bytes and no codec runs on them; the name template turns
/// the device id into a setter the call fills in.
#[derive(Outgoing, Serialized)]
#[outgoing(name = "devices/{device}/state")]
struct DeviceState(Vec<u8>);
// --8<-- [end:state]

#[derive(Debug, Deserialize)]
struct Telemetry {
    device: String,
    temperature: f64,
}

/// The slot this handler announces through.
#[derive(OutSlot)]
#[publishes(DeviceState)]
struct States;

// --8<-- [start:stepped_handler]
/// A reading is one of many; the state it puts the device in is the one a service joining later
/// must see. So the handler publishes the state retained, and only the state: the argument
/// belongs to this message, not to the publisher.
///
/// Naming a per-message setting is the one thing that ties a body to a broker, and its signature
/// says so - the options type is this crate's, and so is the prelude the file imports.
#[subscriber(MqttTopic::new("devices/+/telemetry").qos(Qos::AtLeastOnce))]
async fn announce_state(
    telemetry: &Telemetry,
    Out(states): Out<impl Publisher<Options = MqttPublishOptions>, States>,
) -> HandlerOutcome {
    let state = if telemetry.temperature > 30.0 {
        "hot"
    } else {
        "ok"
    };
    if states
        .message(&DeviceState(state.as_bytes().to_vec()))
        .retain(true)
        .to()
        .device(&telemetry.device)
        .publish()
        .await
        .is_err()
    {
        return HandlerOutcome::retry();
    }
    HandlerOutcome::ack()
}
// --8<-- [end:stepped_handler]

#[ruststream::app]
fn app() -> impl App {
    RustStream::new(AppInfo::new("retained", "0.1.0")).with_broker(
        MqttBroker::new("mqtt://localhost:1883", "retained-example"),
        |b| {
            // --8<-- [start:retained]
            b.after_startup(
                Publish::default().qos(Qos::AtLeastOnce).retain(true),
                async move |publisher| -> Result<(), PublishError<MqttError>> {
                    publisher
                        .message(&DeviceState(b"online".to_vec()))
                        .to()
                        .device("dev42")
                        .publish()
                        .await
                },
            );
            // --8<-- [end:retained]

            // --8<-- [start:per_publish]
            b.after_startup(
                Publish::default(),
                async move |publisher| -> Result<(), PublishError<MqttError>> {
                    publisher
                        .message(&DeviceState(b"online".to_vec()))
                        .retain(true)
                        .to()
                        .device("dev43")
                        .publish()
                        .await
                },
            );
            // --8<-- [end:per_publish]

            // --8<-- [start:stepped_mount]
            // The mount site fixes what every publish through the slot takes; the handler's step
            // is what one message changes.
            b.include(announce_state)
                .out(States, Publish::default().qos(Qos::AtLeastOnce))
                .build();
            // --8<-- [end:stepped_mount]
        },
    )
}
