//! Retained messages: the broker keeps the last message per topic and hands it to new
//! subscribers.
//!
//! Retain is declared for a whole publisher on the publish policy, or for one message on the
//! call; either way the scope's `after_startup` hook runs it once the broker is connected, so
//! the announcement cannot race the connection.
//!
//! Run a broker first (`just brokers-up`), then:
//! `cargo run --example mqtt_retained -- run`

use ruststream::OutgoingMessage;
use ruststream::runtime::PublishError;
// The error type is named here because this example handles it, in the hooks' return types.
use ruststream_rumqttc::MqttError;
use ruststream_rumqttc::prelude::*;

#[ruststream::app]
fn app() -> impl App {
    RustStream::new(AppInfo::new("retained", "0.1.0")).with_broker(
        MqttBroker::new("mqtt://localhost:1883", "retained-example"),
        |b| {
            // --8<-- [start:retained]
            b.after_startup(
                MqttPublish::default().qos(Qos::AtLeastOnce).retain(true),
                async move |publisher| -> Result<(), MqttError> {
                    let msg = OutgoingMessage::new("devices/dev42/state", b"online".as_slice());
                    publisher.publish(msg).await
                },
            );
            // --8<-- [end:retained]

            // --8<-- [start:per_publish]
            b.after_startup(
                MqttPublish::default(),
                async move |publisher| -> Result<(), PublishError<MqttError>> {
                    publisher
                        .with_retain(true)
                        .raw(b"online")
                        .to("devices/dev43/state")
                        .publish()
                        .await
                },
            );
            // --8<-- [end:per_publish]
        },
    )
}
