//! The service of the Publishing guide: operators send commands, and the service forwards each
//! one to its device's own topic, at the quality of service the command needs.
//!
//! ```text
//! cargo test --example guide_publishing --features testing
//! cargo run --example guide_publishing --features testing -- run
//! ```

// The derive and the value a publish transform reads share the name in different namespaces: the
// prelude carries the macro `ruststream::Outgoing`, and this is the type `runtime::Outgoing`.
use ruststream::runtime::{Outgoing, Reads};
use ruststream_rumqttc::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
#[outgoing(name = "operators/commands")]
struct CommandRequest {
    device: String,
    action: String,
    operator: String,
    urgent: bool,
}

// --8<-- [start:command]
/// What a device reads on its own command topic; the `{device}` level becomes a setter.
#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
#[outgoing(name = "devices/{device}/commands")]
struct DeviceCommand {
    action: String,
}

#[derive(OutSlot)]
#[publishes(DeviceCommand)]
struct Commands;
// --8<-- [end:command]

// --8<-- [start:dispatch]
/// Forwards a command to its device. An urgent one goes out at `QoS` 2, every other one at the
/// quality of service of the slot's policy; the operator travels as a header.
#[subscriber("operators/commands")]
async fn dispatch(
    request: &CommandRequest,
    Out(commands): Out<impl Publisher<Options = MqttPublishOptions>, Commands>,
) -> HandlerOutcome {
    let mut headers = HeaderMap::new();
    headers.insert("operator", request.operator.clone());
    let command = DeviceCommand {
        action: request.action.clone(),
    };
    let message = commands.message(&command).with_headers(headers);
    let message = if request.urgent {
        message.qos(Qos::ExactlyOnce)
    } else {
        message
    };
    if let Err(err) = message.to().device(&request.device).publish().await {
        tracing::warn!(request.device, %err, "command refused");
        return HandlerOutcome::drop();
    }
    tracing::info!(request.device, request.action, "command sent");
    HandlerOutcome::ack()
}
// --8<-- [end:dispatch]

// --8<-- [start:app]
/// States the media type of everything the slot sends, which fills the content type property
/// and tells a device to read the body as UTF-8 text.
struct Json;

impl<K: ContextKind, Options> PublishTransform<K, Options> for Json {
    type Destination = Reads;

    fn apply(&self, out: &mut Outgoing<'_>, _options: &mut Option<Options>, _cx: &K::View<'_>) {
        out.headers_mut().insert("content-type", "application/json");
    }
}

#[ruststream::app]
fn app() -> impl App<State = ()> {
    RustStream::new(AppInfo::new("commander", "0.1.0")).with_broker(
        MqttBroker::new("mqtt://localhost:1883", "commander"),
        |b| {
            b.include(dispatch)
                .out(Commands, Publish::default().qos(Qos::AtLeastOnce))
                .transform(Json)
                .build();
        },
    )
}
// --8<-- [end:app]

#[cfg(test)]
mod tests {
    use std::error::Error;

    use ruststream::testing::TestApp;
    use ruststream_rumqttc::prelude::*;

    use super::{CommandRequest, Commands, DeviceCommand, app};

    fn request(device: &str, urgent: bool) -> CommandRequest {
        CommandRequest {
            device: device.to_owned(),
            action: "reboot".to_owned(),
            operator: "ana".to_owned(),
            urgent,
        }
    }

    fn reboot() -> DeviceCommand {
        DeviceCommand {
            action: "reboot".to_owned(),
        }
    }

    #[tokio::test]
    async fn a_routine_command_takes_the_policy_of_the_slot() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;

        tb.broker::<MqttBroker>()
            .message(&request("dev42", false))
            .publish()
            .await?;

        tb.out::<Commands>()
            .assert_called_once()
            .assert_options_default();
        tb.broker::<MqttBroker>()
            .published::<DeviceCommand>("devices/dev42/commands")
            .assert_called_once()
            .with(&reboot())
            .with_header("operator", "ana")
            .with_header("content-type", "application/json");
        tb.shutdown().await?;
        Ok(())
    }

    #[tokio::test]
    async fn an_urgent_command_goes_out_at_qos_2() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;

        tb.broker::<MqttBroker>()
            .message(&request("dev42", true))
            .publish()
            .await?;

        tb.out::<Commands>()
            .assert_called_once()
            .with_options(&MqttPublishOptions::default().qos(Qos::ExactlyOnce));
        tb.broker::<MqttBroker>()
            .published::<DeviceCommand>("devices/dev42/commands")
            .assert_called_once()
            .with(&reboot());
        tb.shutdown().await?;
        Ok(())
    }

    #[tokio::test]
    async fn a_command_for_a_wildcard_device_is_refused() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;

        tb.broker::<MqttBroker>()
            .message(&request("+", false))
            .publish()
            .await?;

        tb.broker::<MqttBroker>()
            .subscriber("operators/commands")
            .assert_called_once()
            .settled(HandlerOutcome::drop());
        tb.broker::<MqttBroker>()
            .published::<DeviceCommand>("devices/+/commands")
            .assert_not_called();
        tb.shutdown().await?;
        Ok(())
    }
}
