//! Retained messages: the broker keeps the last message per topic and hands it to new
//! subscribers.
//!
//! Run a broker first (`just brokers-up`), then:
//! `cargo run --example mqtt_retained`

use ruststream::{Broker, ConnectedBroker, OutgoingMessage, PublishPolicy, Publisher};
use ruststream_rumqttc::{MqttBroker, MqttPublish, Qos};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let connected = MqttBroker::new("mqtt://localhost:1883", "retained-example")
        .connect()
        .await?;

    // The policy names the mode; the runtime pairs it the same way after connect.
    let retained = MqttPublish::default()
        .qos(Qos::AtLeastOnce)
        .retain(true)
        .pair(&connected)
        .await?;
    retained
        .publish(OutgoingMessage::new(
            "devices/dev42/state",
            b"online".as_slice(),
        ))
        .await?;

    connected.shutdown().await?;
    Ok(())
}
