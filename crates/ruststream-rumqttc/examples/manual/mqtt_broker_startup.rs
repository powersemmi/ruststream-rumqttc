//! The service of `examples/mqtt_broker_startup.rs` without the macros: the handler is a named type and `main` is written by hand.
//!
//! ```text
//! cargo test --example manual_mqtt_broker_startup --features testing
//! just brokers-up
//! MQTT_TEST_AUTH_URL=mqtt://127.0.0.1:1884 MQTT_TEST_TLS_URL=mqtts://127.0.0.1:8883 \
//!     MQTT_TEST_TLS_DIR=.stand-tls cargo test --example manual_mqtt_broker_startup --features testing
//! ```

// A plain build compiles the service; the tests that use it exist only in the test build.
#![cfg_attr(not(test), allow(dead_code, unused_imports))]

use std::future::{Future, ready};
use std::process::ExitCode;

use ruststream::runtime::PublishError;
use ruststream::runtime::cli::run_main;
use ruststream::schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use ruststream_rumqttc::MqttError;
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

// --8<-- [start:app]
/// Once the connection is up and every subscription is open, the service announces itself on its
/// status topic, retained for whoever subscribes later.
fn service(broker: MqttBroker) -> impl App<State = ()> {
    RustStream::new(AppInfo::new("telemetry", "0.1.0")).with_broker(broker, |b| {
        b.include(subscriber("broker-startup/telemetry", Record).build());
        b.after_startup(
            Publish::default().retain(true),
            async move |publisher| -> Result<(), PublishError<MqttError>> {
                publisher.message(&Status { online: true }).publish().await
            },
        );
    })
}

fn app() -> impl App<State = ()> {
    service(MqttBroker::new("mqtt://localhost:1883", "telemetry-svc"))
}
// --8<-- [end:app]

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

// `#[outgoing(name = "broker-startup/telemetry")]` by hand: the type names the topic it is published to.
impl OutgoingDestination for Telemetry {
    type Form = FixedName;
    const DESTINATION: &'static str = "broker-startup/telemetry";
}

impl MessageHeaders for Telemetry {
    type Contract = NoHeaders;
}

#[derive(Debug, PartialEq, Deserialize, Serialize, JsonSchema)]
#[schemars(crate = "ruststream::schemars")]
struct Status {
    online: bool,
}

// `#[outgoing(name = "services/telemetry-svc/status")]` by hand: the type names the topic it is published to.
impl OutgoingDestination for Status {
    type Form = FixedName;
    const DESTINATION: &'static str = "services/telemetry-svc/status";
}

impl MessageHeaders for Status {
    type Contract = NoHeaders;
}

#[cfg(test)]
mod tests {
    use std::error::Error;
    use std::fs;
    use std::time::{Duration, Instant};

    use ruststream::testing::TestApp;
    use ruststream_rumqttc::MqttError;
    use ruststream_rumqttc::prelude::*;

    use super::{Status, app, live, service};

    /// The `MqttError` a failed startup carries, found down its chain of causes.
    fn mqtt_error<'e>(err: &'e (dyn Error + 'static)) -> Option<&'e MqttError> {
        let mut cause = Some(err);
        while let Some(err) = cause {
            if let Some(mqtt) = err.downcast_ref::<MqttError>() {
                return Some(mqtt);
            }
            cause = err.source();
        }
        None
    }

    #[tokio::test]
    async fn the_service_announces_itself_once_it_has_started() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;

        tb.broker::<MqttBroker>()
            .published::<Status>("services/telemetry-svc/status")
            .assert_called_once()
            .with(&Status { online: true });
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_refused_login_fails_startup_at_once() -> Result<(), Box<dyn Error>> {
        let Some(url) = live::url("MQTT_TEST_AUTH_URL") else {
            return Ok(());
        };
        let broker = MqttBroker::new(&url, live::client_id("broker-startup-login"))
            .credentials("tester", "wrong");

        let started = Instant::now();
        let failed = TestApp::start_live(service(broker)).await.err();

        let failed = failed.ok_or("the server accepted a wrong password")?;
        let mqtt = mqtt_error(&failed).ok_or("startup failed without an MqttError")?;
        assert!(matches!(mqtt, MqttError::Connect(_)), "{mqtt}");
        assert!(mqtt.to_string().contains("refused"), "{mqtt}");
        assert!(
            started.elapsed() < Duration::from_secs(5),
            "{:?}",
            started.elapsed()
        );
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_server_certificate_nobody_trusts_fails_startup_at_once() -> Result<(), Box<dyn Error>>
    {
        let Some((url, dir)) = live::tls() else {
            return Ok(());
        };
        // The client's own certificate as the authority: real, but it signed nothing the server
        // presents.
        let broker = MqttBroker::new(&url, live::client_id("broker-startup-tls"))
            .tls_ca(fs::read(dir.join("client.crt"))?)
            .tls_client_auth(
                fs::read(dir.join("client.crt"))?,
                fs::read(dir.join("client.key"))?,
            );

        let started = Instant::now();
        let failed = TestApp::start_live(service(broker)).await.err();

        let failed = failed.ok_or("the handshake succeeded with the wrong authority")?;
        let mqtt = mqtt_error(&failed).ok_or("startup failed without an MqttError")?;
        assert!(mqtt.to_string().contains("tls handshake failed"), "{mqtt}");
        assert!(
            started.elapsed() < Duration::from_secs(5),
            "{:?}",
            started.elapsed()
        );
        Ok(())
    }
}
