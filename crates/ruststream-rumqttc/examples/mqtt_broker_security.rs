//! Security: a user name and a password with `credentials`, and TLS with the server's certificate
//! authority and the service's own certificate. The in-process tests run everywhere; the live ones
//! run against the stand's password and TLS listeners and skip without them.
//!
//! ```text
//! cargo test --example mqtt_broker_security --features testing
//! just brokers-up
//! MQTT_TEST_AUTH_URL=mqtt://127.0.0.1:1884 MQTT_TEST_AUTH_PASSWORD=s3cret \
//!     MQTT_TEST_TLS_URL=mqtts://127.0.0.1:8883 \
//!     MQTT_TEST_TLS_DIR=.stand-tls cargo test --example mqtt_broker_security --features testing
//! ```

// A plain build compiles the service; the tests that use it exist only in the test build.
#![cfg_attr(not(test), allow(dead_code, unused_imports))]

use std::path::Path;
use std::{env, fs, io};

use serde::{Deserialize, Serialize};

use ruststream_rumqttc::prelude::*;

#[cfg(test)]
#[path = "../tests/live_broker/mod.rs"]
mod live;

#[subscriber("broker-security/telemetry")]
async fn record(telemetry: &Telemetry) -> HandlerOutcome {
    tracing::info!(telemetry.device, telemetry.temperature, "reading recorded");
    HandlerOutcome::ack()
}

fn service(broker: MqttBroker) -> impl App<State = ()> {
    RustStream::new(AppInfo::new("telemetry", "0.1.0")).with_broker(broker, |b| {
        b.include(record);
    })
}

// --8<-- [start:password]
/// A server that asks every client for a user name and a password.
fn password_broker(url: &str, client_id: &str, username: &str, password: &str) -> MqttBroker {
    MqttBroker::new(url, client_id).credentials(username, password)
}

#[ruststream::app]
fn app() -> impl App<State = ()> {
    let password = env::var("MQTT_PASSWORD").unwrap_or_default();
    service(password_broker(
        "mqtt://localhost:1884",
        "telemetry-svc",
        "telemetry-svc",
        &password,
    ))
}
// --8<-- [end:password]

// --8<-- [start:url]
/// The deployment hands over one connection string with the login in it, such as
/// `mqtt://telemetry-svc:p%40ss@broker.internal:1884` for the password `p@ss`.
fn url_broker(url: &str, client_id: &str) -> MqttBroker {
    MqttBroker::new(url, client_id)
}
// --8<-- [end:url]

// --8<-- [start:tls]
/// A TLS listener that verifies the service by its certificate. `dir` holds the PEM files the
/// deployment mounts: the authority that signed the server's certificate, and the service's own
/// certificate and key.
fn tls_broker(url: &str, client_id: &str, dir: &Path) -> io::Result<MqttBroker> {
    Ok(MqttBroker::new(url, client_id)
        .tls_ca(fs::read(dir.join("ca.crt"))?)
        .tls_client_auth(
            fs::read(dir.join("client.crt"))?,
            fs::read(dir.join("client.key"))?,
        ))
}
// --8<-- [end:tls]

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
#[outgoing(name = "broker-security/telemetry")]
struct Telemetry {
    device: String,
    temperature: f64,
}

#[cfg(test)]
mod tests {
    use std::error::Error;
    use std::hash::{BuildHasher, Hasher, RandomState};

    use ruststream::testing::TestApp;
    use ruststream_rumqttc::prelude::*;

    use super::{Telemetry, app, live, password_broker, service, tls_broker, url_broker};

    /// A secret made up at run time, so none is written into the source.
    fn made_up_secret() -> String {
        RandomState::new().build_hasher().finish().to_string()
    }

    /// The body every mode runs: a reading reaches the handler through the secured connection.
    async fn records_a_reading(tb: TestApp<()>) -> Result<(), Box<dyn Error>> {
        let reading = Telemetry {
            device: "dev42".to_owned(),
            temperature: 21.5,
        };
        tb.broker::<MqttBroker>()
            .message(&reading)
            .publish()
            .await?;

        tb.broker::<MqttBroker>()
            .subscriber("broker-security/telemetry")
            .assert_called_once()
            .with(&reading)
            .settled(HandlerOutcome::ack());
        tb.shutdown().await?;
        Ok(())
    }

    #[tokio::test]
    async fn the_service_with_credentials_runs_in_process() -> Result<(), Box<dyn Error>> {
        records_a_reading(TestApp::start(app()).await?).await
    }

    #[tokio::test]
    async fn a_tls_address_without_an_authority_does_not_start() {
        let broker = password_broker(
            "mqtts://localhost:8883",
            "telemetry-svc",
            "telemetry-svc",
            &made_up_secret(),
        );

        let refused = TestApp::start(service(broker)).await.err();
        let refused = refused.map(|err| err.to_string()).unwrap_or_default();
        assert!(refused.contains("tls_ca"), "{refused}");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn the_password_listener_accepts_the_credentials() -> Result<(), Box<dyn Error>> {
        let Some((url, password)) = live::auth() else {
            return Ok(());
        };
        let id = live::client_id("broker-security-password");
        let broker = password_broker(&url, &id, "tester", &password);
        records_a_reading(TestApp::start_live(service(broker)).await?).await
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn the_password_listener_accepts_the_login_in_the_url() -> Result<(), Box<dyn Error>> {
        let Some(url) = live::url("MQTT_TEST_AUTH_URL") else {
            return Ok(());
        };
        // The stand's password, percent-encoded the way a connection string writes it.
        let url = url.replacen("://", "://tester:s%33cret@", 1);
        let id = live::client_id("broker-security-url");
        records_a_reading(TestApp::start_live(service(url_broker(&url, &id))).await?).await
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn the_tls_listener_accepts_the_certificates() -> Result<(), Box<dyn Error>> {
        let Some((url, dir)) = live::tls() else {
            return Ok(());
        };
        let id = live::client_id("broker-security-tls");
        let broker = tls_broker(&url, &id, &dir)?;
        records_a_reading(TestApp::start_live(service(broker)).await?).await
    }
}
