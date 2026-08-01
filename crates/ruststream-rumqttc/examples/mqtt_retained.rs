//! Retained messages: the broker keeps the last message per topic and hands it to new
//! subscribers.
//!
//! The retain flag is pure declaration on the publish policy; the scope's `after_startup` hook
//! runs it once the broker is connected, so the announcement cannot race the connection.
//!
//! Run a broker first (`just brokers-up`), then:
//! `cargo run --example mqtt_retained -- run`

use ruststream::runtime::{App, AppInfo, RustStream};
use ruststream::{OutgoingMessage, Publisher};
use ruststream_rumqttc::{MqttBroker, MqttError, MqttPublish, Qos};

#[ruststream::app]
fn app() -> impl App {
    RustStream::new(AppInfo::new("retained", "0.1.0")).with_broker(
        MqttBroker::new("mqtt://localhost:1883", "retained-example"),
        |b| {
            b.after_startup(
                MqttPublish::default().qos(Qos::AtLeastOnce).retain(true),
                async move |publisher| -> Result<(), MqttError> {
                    let msg = OutgoingMessage::new("devices/dev42/state", b"online".as_slice());
                    publisher.publish(msg).await
                },
            );
        },
    )
}
