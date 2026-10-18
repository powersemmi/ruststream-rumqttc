//! The last will: the message the server publishes for a service whose connection ends without a
//! clean disconnect. Only a server publishes it, so the tests run against the stand and skip
//! without it.
//!
//! ```text
//! cargo test --example mqtt_broker_last_will --features testing
//! just brokers-up
//! MQTT_TEST_URL=mqtt://127.0.0.1:1883 cargo test --example mqtt_broker_last_will --features testing
//! ```

// A plain build compiles the service; the tests that use it exist only in the test build.
#![cfg_attr(not(test), allow(dead_code, unused_imports))]

use serde::{Deserialize, Serialize};

use ruststream_rumqttc::prelude::*;

#[cfg(test)]
#[path = "../tests/live_broker/mod.rs"]
mod live;

#[subscriber("broker-last-will/telemetry")]
async fn record(telemetry: &Telemetry) -> HandlerOutcome {
    tracing::info!(telemetry.device, telemetry.temperature, "reading recorded");
    HandlerOutcome::ack()
}

fn service(broker: MqttBroker) -> impl App<State = ()> {
    RustStream::new(AppInfo::new("telemetry", "0.1.0")).with_broker(broker, |b| {
        b.include(record);
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

#[ruststream::app]
fn app() -> impl App<State = ()> {
    service(broker("mqtt://localhost:1883", "telemetry-svc"))
}
// --8<-- [end:broker]

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
#[outgoing(name = "broker-last-will/telemetry")]
struct Telemetry {
    device: String,
    temperature: f64,
}

#[cfg(test)]
mod tests {
    use std::error::Error;

    use ruststream::testing::TestApp;
    use ruststream_rumqttc::prelude::*;
    use serde::{Deserialize, Serialize};

    use super::{Telemetry, app, broker, live, service};

    #[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
    struct Status {
        online: bool,
    }

    /// The client id of the service the tests stop, unique to the run.
    fn service_id() -> String {
        live::client_id("broker-last-will")
    }

    fn status_topic() -> String {
        format!("services/{}/status", service_id())
    }

    /// What a dashboard runs: it reads the service's status topic.
    #[subscriber(MqttTopic::new(status_topic()))]
    async fn watch(status: &Status) -> HandlerOutcome {
        tracing::info!(status.online, "status changed");
        HandlerOutcome::ack()
    }

    fn dashboard(url: &str) -> impl App<State = ()> {
        RustStream::new(AppInfo::new("dashboard", "0.1.0")).with_broker(
            MqttBroker::new(url, live::client_id("broker-last-will-dashboard")),
            |b| {
                b.include(watch);
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
