//! The service of `examples/mqtt_broker_session.rs` without the macros: the handler is a named type and `main` is written by hand.
//!
//! ```text
//! cargo test --example manual_mqtt_broker_session --features testing
//! just brokers-up
//! MQTT_TEST_URL=mqtt://127.0.0.1:1883 cargo test --example manual_mqtt_broker_session --features testing
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
        b.include(subscriber("broker-session/telemetry", Record).build());
    })
}

// --8<-- [start:broker]
/// The server at `url` keeps this client's session for a day after the service disconnects.
fn broker(url: &str, client_id: &str) -> MqttBroker {
    MqttBroker::new(url, client_id)
        .clean_start(false)
        .session_expiry(Duration::from_hours(24))
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

// `#[outgoing(name = "broker-session/telemetry")]` by hand: the type names the topic it is published to.
impl OutgoingDestination for Telemetry {
    type Form = FixedName;
    const DESTINATION: &'static str = "broker-session/telemetry";
}

impl MessageHeaders for Telemetry {
    type Contract = NoHeaders;
}

#[cfg(test)]
mod tests {
    use std::error::Error;

    use ruststream::testing::TestApp;
    use ruststream_rumqttc::prelude::*;

    use super::{Telemetry, app, broker, live, service};

    fn reading(temperature: f64) -> Telemetry {
        Telemetry {
            device: "dev42".to_owned(),
            temperature,
        }
    }

    #[tokio::test]
    async fn the_service_with_a_persistent_session_runs_in_process() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;
        tb.broker::<MqttBroker>()
            .message(&reading(21.5))
            .publish()
            .await?;

        tb.broker::<MqttBroker>()
            .subscriber("broker-session/telemetry")
            .assert_called_once()
            .settled(HandlerOutcome::ack());
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_reading_published_while_the_service_was_down_arrives_on_return()
    -> Result<(), Box<dyn Error + Send + Sync>> {
        let Some(url) = live::url("MQTT_TEST_URL") else {
            return Ok(());
        };
        let id = live::client_id("broker-session");

        // The first run opens the session and its subscription, then the process dies.
        let crash_url = url.clone();
        let crash_id = id.clone();
        live::crash(move || service(broker(&crash_url, &crash_id)))?;

        // Another client publishes while the service is down.
        let sender = RustStream::new(AppInfo::new("sender", "0.1.0")).register_broker(
            MqttBroker::new(&url, live::client_id("broker-session-sender")),
        );
        let sender = TestApp::start_live(sender).await?;
        sender
            .broker::<MqttBroker>()
            .message(&reading(30.5))
            .publish()
            .await?;
        sender.shutdown().await?;

        // The second run resumes the session: the server sends the reading it kept before the one
        // published now.
        let tb = TestApp::start_live(service(broker(&url, &id))).await?;
        tb.broker::<MqttBroker>()
            .message(&reading(21.5))
            .publish()
            .await?;
        let received = tb
            .broker::<MqttBroker>()
            .subscriber("broker-session/telemetry")
            .received::<Telemetry>();
        assert_eq!(received.first(), Some(&reading(30.5)), "{received:?}");
        tb.shutdown().await?;

        // A clean start under the same id ends the session the test left on the server.
        TestApp::start_live(service(MqttBroker::new(&url, &id)))
            .await?
            .shutdown()
            .await?;
        Ok(())
    }
}
