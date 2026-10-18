//! The service of `examples/mqtt_broker_last_will.rs` without the macros: the handler is a named type and `main` is written by hand.
//!
//! ```text
//! cargo test --example manual_mqtt_broker_last_will --features testing
//! just brokers-up
//! MQTT_TEST_URL=mqtt://127.0.0.1:1883 cargo test --example manual_mqtt_broker_last_will --features testing
//! ```

// A plain build compiles the service; the tests that use it exist only in the test build.
#![cfg_attr(not(test), allow(dead_code, unused_imports))]

use std::future::{Future, ready};
use std::process::ExitCode;

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
        b.include(subscriber("broker-last-will/telemetry", Record).build());
    })
}

// --8<-- [start:broker]
/// If the connection ends without a clean disconnect, the server publishes `{"online":false}`
/// on the service's status topic and keeps it there for every later subscriber.
fn broker(url: &str, client_id: &str) -> MqttBroker {
    MqttBroker::new(url, client_id).last_will(
        format!("services/{client_id}/status"),
        r#"{"online":false}"#,
        Qos::AtLeastOnce,
        true,
    )
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

// `#[outgoing(name = "broker-last-will/telemetry")]` by hand: the type names the topic it is published to.
impl OutgoingDestination for Telemetry {
    type Form = FixedName;
    const DESTINATION: &'static str = "broker-last-will/telemetry";
}

impl MessageHeaders for Telemetry {
    type Contract = NoHeaders;
}

#[cfg(test)]
mod tests {
    use std::error::Error;

    use std::future::{Future, ready};

    use ruststream::schemars::JsonSchema;
    use ruststream::testing::TestApp;
    use ruststream_rumqttc::prelude::*;
    use serde::{Deserialize, Serialize};

    use super::{Telemetry, app, broker, live, service};

    #[derive(Debug, PartialEq, Deserialize, Serialize, JsonSchema)]
    #[schemars(crate = "ruststream::schemars")]
    struct Status {
        online: bool,
    }

    impl OutgoingDestination for Status {
        type Form = CallerName;
    }

    impl MessageHeaders for Status {
        type Contract = NoHeaders;
    }

    /// The client id of the service the tests stop, unique to the run.
    fn service_id() -> String {
        live::client_id("broker-last-will")
    }

    fn status_topic() -> String {
        format!("services/{}/status", service_id())
    }

    /// What a dashboard runs: it reads the service's status topic.
    struct Watch;

    impl Handle<Status> for Watch {
        fn handle(
            &self,
            status: &Status,
            _outs: &(),
            _ctx: &mut Context<'_>,
        ) -> impl Future<Output = Result<(), HandlerOutcome>> {
            tracing::info!(status.online, "status changed");
            ready(Ok(()))
        }
    }

    fn dashboard(url: &str) -> impl App<State = ()> {
        RustStream::new(AppInfo::new("dashboard", "0.1.0")).with_broker(
            MqttBroker::new(url, live::client_id("broker-last-will-dashboard")),
            |b| {
                b.include(subscriber(status_topic(), Watch).build());
            },
        )
    }

    #[tokio::test]
    async fn the_service_with_a_last_will_runs_in_process() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;
        let reading = Telemetry {
            device: "dev42".to_owned(),
            temperature: 21.5,
        };
        tb.broker::<MqttBroker>()
            .message(&reading)
            .publish()
            .await?;

        tb.broker::<MqttBroker>()
            .subscriber("broker-last-will/telemetry")
            .assert_called_once()
            .settled(HandlerOutcome::ack());
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn the_server_publishes_the_will_of_a_crash_and_not_of_a_shutdown()
    -> Result<(), Box<dyn Error + Send + Sync>> {
        let Some(url) = live::url("MQTT_TEST_URL") else {
            return Ok(());
        };
        let tb = TestApp::start_live(dashboard(&url)).await?;
        let topic = status_topic();

        // A clean shutdown sends a DISCONNECT, and the server publishes no will. The status the
        // test publishes next is the first one the dashboard reads.
        TestApp::start_live(service(broker(&url, &service_id())))
            .await?
            .shutdown()
            .await?;
        let online = Status { online: true };
        tb.broker::<MqttBroker>()
            .message(&online)
            .to(&topic)
            .publish()
            .await?;
        let received = tb
            .broker::<MqttBroker>()
            .subscriber(&topic)
            .received::<Status>();
        assert_eq!(received.first(), Some(&online), "{received:?}");

        // A crash ends the connection without one, and the server publishes the will before the
        // status the test publishes after it.
        let url_for_crash = url.clone();
        live::crash(move || service(broker(&url_for_crash, &service_id())))?;
        tb.broker::<MqttBroker>()
            .message(&online)
            .to(&topic)
            .publish()
            .await?;
        let received = tb
            .broker::<MqttBroker>()
            .subscriber(&topic)
            .received::<Status>();
        assert_eq!(
            received.get(1),
            Some(&Status { online: false }),
            "{received:?}"
        );

        tb.shutdown().await?;
        Ok(())
    }
}
