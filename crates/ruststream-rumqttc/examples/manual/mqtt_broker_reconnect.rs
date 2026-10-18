//! The service of `examples/mqtt_broker_reconnect.rs` without the macros: the handler is a named type and `main` is written by hand.
//!
//! ```text
//! cargo test --example manual_mqtt_broker_reconnect --features testing
//! just brokers-up
//! MQTT_TEST_URL=mqtt://127.0.0.1:1883 cargo test --example manual_mqtt_broker_reconnect --features testing
//! ```

// A plain build compiles the service; the tests that use it exist only in the test build.
#![cfg_attr(not(test), allow(dead_code, unused_imports))]

use std::future::{Future, ready};
use std::process::ExitCode;
use std::time::Duration;

use ruststream::runtime::cli::run_main;
use ruststream::schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use ruststream_rumqttc::prelude::*;

#[cfg(test)]
#[path = "../../tests/live_broker/mod.rs"]
mod live;

struct Record;

impl Handle<Telemetry> for Record {
    fn handle(
        &self,
        telemetry: &Telemetry,
        _outs: &(),
        _ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<(), HandlerOutcome>> {
        tracing::info!(telemetry.device, telemetry.temperature, "reading recorded");
        ready(Ok(()))
    }
}

fn service(broker: MqttBroker) -> impl App<State = ()> {
    RustStream::new(AppInfo::new("telemetry", "0.1.0")).with_broker(broker, |b| {
        b.include(subscriber("broker-reconnect/telemetry", Record).build());
    })
}

// --8<-- [start:broker]
/// A connection silent for 15 seconds is probed, and one that stays silent is dropped and dialed
/// again. The session outlives the gap by an hour, so what arrives meanwhile is kept.
fn broker(url: &str, client_id: &str) -> MqttBroker {
    MqttBroker::new(url, client_id)
        .keep_alive(Duration::from_secs(15))
        .clean_start(false)
        .session_expiry(Duration::from_hours(1))
}

fn app() -> impl App<State = ()> {
    service(broker("mqtt://localhost:1883", "telemetry-svc"))
}
// --8<-- [end:broker]

// What `#[ruststream::app]` writes: the command line on a multi-threaded Tokio runtime.
fn main() -> ExitCode {
    run_main(app)
}

#[derive(Debug, PartialEq, Deserialize, Serialize, JsonSchema)]
#[schemars(crate = "ruststream::schemars")]
struct Telemetry {
    device: String,
    temperature: f64,
}

// `#[outgoing(name = "broker-reconnect/telemetry")]` by hand: the type names the topic it is published to.
impl OutgoingDestination for Telemetry {
    type Form = FixedName;
    const DESTINATION: &'static str = "broker-reconnect/telemetry";
}

impl MessageHeaders for Telemetry {
    type Contract = NoHeaders;
}

#[cfg(test)]
mod tests {
    use std::error::Error;
    use std::time::Duration;

    use ruststream::testing::TestApp;
    use ruststream_rumqttc::prelude::*;

    use super::{Telemetry, app, broker, live, service};

    fn reading() -> Telemetry {
        Telemetry {
            device: "dev42".to_owned(),
            temperature: 21.5,
        }
    }

    #[tokio::test]
    async fn a_keep_alive_under_five_seconds_does_not_start() {
        let broker =
            broker("mqtt://localhost:1883", "telemetry-svc").keep_alive(Duration::from_secs(2));

        let refused = TestApp::start(service(broker)).await.err();
        let refused = refused.map(|err| err.to_string()).unwrap_or_default();
        assert!(refused.contains("at least 5 seconds"), "{refused}");
    }

    #[tokio::test]
    async fn the_service_runs_in_process() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;
        tb.broker::<MqttBroker>()
            .message(&reading())
            .publish()
            .await?;

        tb.broker::<MqttBroker>()
            .subscriber("broker-reconnect/telemetry")
            .assert_called_once()
            .settled(HandlerOutcome::ack());
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn the_subscription_comes_back_after_the_connection_is_lost()
    -> Result<(), Box<dyn Error + Send + Sync>> {
        let Some(url) = live::url("MQTT_TEST_URL") else {
            return Ok(());
        };
        let id = live::client_id("broker-reconnect");
        let tb = TestApp::start_live(service(broker(&url, &id))).await?;

        // A second process under the same client id takes the session over, so the server closes
        // the service's connection; that process then dies and leaves the session to whoever
        // comes back for it.
        let twin_url = url.clone();
        let twin_id = id.clone();
        live::crash(move || service(broker(&twin_url, &twin_id)))?;

        // The publish leaves once the service has reconnected, and the resumed session delivers it.
        tb.broker::<MqttBroker>()
            .message(&reading())
            .publish()
            .await?;
        tb.broker::<MqttBroker>()
            .subscriber("broker-reconnect/telemetry")
            .assert_called_once()
            .with(&reading())
            .settled(HandlerOutcome::ack());
        tb.shutdown().await?;

        // A clean start under the same id ends the session the test left on the server.
        TestApp::start_live(service(MqttBroker::new(&url, &id)))
            .await?
            .shutdown()
            .await?;
        Ok(())
    }
}
