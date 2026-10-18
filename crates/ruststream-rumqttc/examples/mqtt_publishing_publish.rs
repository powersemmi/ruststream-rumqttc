//! The MQTT publish policy: `Publish` in the prelude sets the quality of service and the retain
//! flag of what a position sends, and a reply mounted without one sends at QoS 1, not retained.
//!
//! ```text
//! cargo test --example mqtt_publishing_publish --features testing
//! cargo run --example mqtt_publishing_publish -- run
//! ```

use ruststream::schemars::JsonSchema;
use ruststream_rumqttc::prelude::*;
use serde::{Deserialize, Serialize};

/// A command sent to one device.
#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing, JsonSchema)]
#[schemars(crate = "ruststream::schemars")]
#[outgoing(name = "devices/dev42/commands")]
struct Command {
    action: String,
}

/// The device's answer to a command, on one topic wherever the handler is mounted.
#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing, JsonSchema)]
#[schemars(crate = "ruststream::schemars")]
#[outgoing(name = "devices/dev42/acks")]
struct CommandAck {
    action: String,
}

/// A liveness probe and its answer.
#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing, JsonSchema)]
#[schemars(crate = "ruststream::schemars")]
#[outgoing(name = "devices/dev42/ping")]
struct Ping {
    seq: u64,
}

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing, JsonSchema)]
#[schemars(crate = "ruststream::schemars")]
#[outgoing(name = "devices/dev42/pong")]
struct Pong {
    seq: u64,
}

// --8<-- [start:handlers]
#[subscriber(MqttTopic::new("devices/dev42/commands"), reply)]
async fn acknowledge(command: &Command) -> CommandAck {
    tracing::info!(command.action, "command applied");
    CommandAck {
        action: command.action.clone(),
    }
}

#[subscriber(MqttTopic::new("devices/dev42/ping"), reply)]
async fn pong(ping: &Ping) -> Pong {
    Pong { seq: ping.seq }
}
// --8<-- [end:handlers]

// --8<-- [start:app]
#[ruststream::app]
fn app() -> impl App {
    RustStream::new(AppInfo::new("device-agent", "0.1.0")).with_broker(
        MqttBroker::new("mqtt://localhost:1883", "device-agent-dev42"),
        |b| {
            // An acknowledgement must arrive exactly once.
            b.include(acknowledge)
                .out_reply(Publish::default().qos(Qos::ExactlyOnce));
            // No policy named: the reply goes through the broker's default one.
            b.include(pong);
        },
    )
}
// --8<-- [end:app]

#[cfg(test)]
mod tests {
    use std::error::Error;

    use ruststream::testing::TestApp;
    use ruststream_rumqttc::prelude::*;

    use super::{Command, CommandAck, Ping, Pong, app};

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn each_reply_lands_on_the_topic_its_type_names() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;
        tb.broker::<MqttBroker>()
            .message(&Command {
                action: "reboot".to_owned(),
            })
            .publish()
            .await?;
        tb.broker::<MqttBroker>()
            .message(&Ping { seq: 7 })
            .publish()
            .await?;

        tb.broker::<MqttBroker>()
            .published::<CommandAck>("devices/dev42/acks")
            .assert_called_once()
            .with(&CommandAck {
                action: "reboot".to_owned(),
            });
        tb.broker::<MqttBroker>()
            .published::<Pong>("devices/dev42/pong")
            .assert_called_once()
            .with(&Pong { seq: 7 });
        tb.shutdown().await?;
        Ok(())
    }
}
