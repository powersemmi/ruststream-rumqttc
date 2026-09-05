//! Retained messages: the broker keeps the last message per topic and hands it to new
//! subscribers.
//!
//! Retain is declared for a whole publisher on the publish policy, or for one message on the
//! call; either way the scope's `after_startup` hook runs it once the broker is connected, so
//! the announcement cannot race the connection.
//!
//! Run a broker first (`just brokers-up`), then:
//! `cargo run --example mqtt_retained -- run`

use ruststream::runtime::PublishError;
// The error type is named here because this example handles it, in the hooks' return types.
use ruststream_rumqttc::MqttError;
use ruststream_rumqttc::prelude::*;

// --8<-- [start:state]
/// A device state announcement. An MQTT state is bytes on the wire rather than an encoded
/// model, so the type carries its own bytes and no codec runs on them; the name template turns
/// the device id into a setter the call fills in.
#[derive(Outgoing, Serialized)]
#[outgoing(name = "devices/{device}/state")]
struct DeviceState(Vec<u8>);
// --8<-- [end:state]

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
                        .with_retain(true)
                        .message(&DeviceState(b"online".to_vec()))
                        .to()
                        .device("dev43")
                        .publish()
                        .await
                },
            );
            // --8<-- [end:per_publish]
        },
    )
}
