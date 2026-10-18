//! Headers on MQTT: they travel as MQTT 5 properties, and a publish transform sets the content
//! type of everything a position sends.
//!
//! ```text
//! cargo test --example mqtt_publishing_headers --features testing
//! cargo run --example mqtt_publishing_headers -- run
//! ```

// The derive and the value a publish transform reads share the name in different namespaces: the
// prelude carries the macro `ruststream::Outgoing`, and this is the type `runtime::Outgoing`.
use ruststream::runtime::{Outgoing, Reads};
use ruststream::schemars::JsonSchema;
use ruststream_rumqttc::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing, JsonSchema)]
#[schemars(crate = "ruststream::schemars")]
#[outgoing(name = "devices/dev42/telemetry")]
struct Telemetry {
    temperature: f64,
}

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing, JsonSchema)]
#[schemars(crate = "ruststream::schemars")]
#[outgoing(name = "devices/dev42/state")]
struct DeviceState {
    hot: bool,
}

// --8<-- [start:transform]
/// States the media type of every message the position sends. The header fills the content type
/// property and sets the payload format indicator, so a peer reads the body as UTF-8 JSON. The
/// transform reads nothing of the delivery, so it mounts on any position.
struct Json;

impl<K: ContextKind, Options> PublishTransform<K, Options> for Json {
    type Destination = Reads;

    fn apply(&self, out: &mut Outgoing<'_>, _options: &mut Option<Options>, _cx: &K::View<'_>) {
        out.headers_mut().insert("content-type", "application/json");
    }
}

#[subscriber(MqttTopic::new("devices/dev42/telemetry"), reply)]
async fn track(telemetry: &Telemetry) -> DeviceState {
    DeviceState {
        hot: telemetry.temperature > 30.0,
    }
}

#[ruststream::app]
fn app() -> impl App {
    RustStream::new(AppInfo::new("states", "0.1.0")).with_broker(
        MqttBroker::new("mqtt://localhost:1883", "states-svc"),
        |b| {
            b.include(track)
                .out_reply(Publish::default())
                .transform(Json);
        },
    )
}
// --8<-- [end:transform]

#[cfg(test)]
mod tests {
    use std::error::Error;

    use ruststream::runtime::{Outgoing, Reads};
    use ruststream::testing::TestApp;
    use ruststream_rumqttc::prelude::*;

    use super::{DeviceState, Telemetry, app, track};

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn every_reply_states_its_content_type() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;
        tb.broker::<MqttBroker>()
            .message(&Telemetry { temperature: 31.5 })
            .publish()
            .await?;

        tb.broker::<MqttBroker>()
            .published::<DeviceState>("devices/dev42/state")
            .assert_called_once()
            .with(&DeviceState { hot: true })
            .with_header("content-type", "application/json");
        tb.shutdown().await?;
        Ok(())
    }

    /// Marks a reply with the site it came from, in a value that carries a control character.
    struct Unprintable;

    impl<K: ContextKind, Options> PublishTransform<K, Options> for Unprintable {
        type Destination = Reads;

        fn apply(&self, out: &mut Outgoing<'_>, _options: &mut Option<Options>, _cx: &K::View<'_>) {
            out.headers_mut().insert("x-site", "plant\u{0}1");
        }
    }

    /// A property is an MQTT string, so a header with a control character refuses the publish
    /// instead of reaching a server that may close the connection on it.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_header_that_is_not_mqtt_text_refuses_the_publish() -> Result<(), Box<dyn Error>> {
        let app = RustStream::new(AppInfo::new("states", "0.1.0")).with_broker(
            MqttBroker::new("mqtt://localhost:1883", "states-svc"),
            |b| {
                b.include(track)
                    .out_reply(Publish::default())
                    .transform(Unprintable);
            },
        );
        let tb = TestApp::start(app).await?;
        tb.broker::<MqttBroker>()
            .message(&Telemetry { temperature: 21.5 })
            .publish()
            .await?;

        tb.broker::<MqttBroker>()
            .subscriber("devices/dev42/telemetry")
            .assert_called_once();
        tb.broker::<MqttBroker>()
            .published::<DeviceState>("devices/dev42/state")
            .assert_not_called();
        tb.shutdown().await?;
        Ok(())
    }
}
