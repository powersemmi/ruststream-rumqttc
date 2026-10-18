//! The service of the Production guide: a telemetry service configured for a managed MQTT
//! service that takes a password and a client certificate over TLS, with every setting read from
//! the environment.
//!
//! ```text
//! MQTT_URL=mqtts://mqtt.example.com:8883 MQTT_CLIENT_ID=telemetry-1 MQTT_USERNAME=telemetry \
//!     MQTT_PASSWORD=... MQTT_CA=ca.pem MQTT_CERT=client.pem MQTT_KEY=client.key \
//!     cargo run --example guide_production --features testing -- run
//! ```

use std::time::Duration;
use std::{env, fs};

use ruststream_rumqttc::prelude::*;
use serde::{Deserialize, Serialize};

// --8<-- [start:broker]
/// Where the server is and who the service is, as the deployment provides them.
struct Deployment {
    url: String,
    client_id: String,
    username: String,
    password: String,
    ca: Vec<u8>,
    cert: Vec<u8>,
    key: Vec<u8>,
}

impl Deployment {
    fn from_env() -> Self {
        let read = |name: &str| env::var(name).unwrap_or_default();
        let file = |name: &str| fs::read(read(name)).unwrap_or_default();
        Self {
            url: read("MQTT_URL"),
            client_id: read("MQTT_CLIENT_ID"),
            username: read("MQTT_USERNAME"),
            password: read("MQTT_PASSWORD"),
            ca: file("MQTT_CA"),
            cert: file("MQTT_CERT"),
            key: file("MQTT_KEY"),
        }
    }
}

fn broker(deployment: Deployment) -> MqttBroker {
    MqttBroker::new(deployment.url, deployment.client_id)
        .credentials(deployment.username, deployment.password)
        .tls_ca(deployment.ca)
        .tls_client_auth(deployment.cert, deployment.key)
        .keep_alive(Duration::from_secs(30))
        .max_packet_size(256 * 1024)
        .receive_maximum(200)
}
// --8<-- [end:broker]

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
struct Telemetry {
    device: String,
    temperature: f64,
}

// --8<-- [start:service]
#[subscriber(MqttFilter::new("devices/+/telemetry").qos(Qos::AtLeastOnce).shared("ingest"))]
async fn record(telemetry: &Telemetry) -> HandlerOutcome {
    tracing::info!(telemetry.device, telemetry.temperature, "reading recorded");
    HandlerOutcome::ack()
}

fn service(broker: MqttBroker) -> RustStream {
    RustStream::new(AppInfo::new("telemetry", "0.1.0")).with_broker(broker, |b| {
        b.include(record)
            .out_retry(Publish::default())
            .to("devices/retry/telemetry");
    })
}

#[ruststream::app]
fn app() -> impl App<State = ()> {
    service(broker(Deployment::from_env()))
}
// --8<-- [end:service]

#[cfg(test)]
mod tests {
    use std::error::Error;
    use std::time::Duration;

    use ruststream::testing::TestApp;
    use ruststream_rumqttc::prelude::*;

    use super::{Deployment, Telemetry, broker, service};

    fn deployment() -> Deployment {
        Deployment {
            url: "mqtts://mqtt.example.com:8883".to_owned(),
            client_id: "telemetry-1".to_owned(),
            username: "telemetry".to_owned(),
            password: "s3cret-pa55".to_owned(),
            ca: b"-----BEGIN CERTIFICATE-----".to_vec(),
            cert: b"-----BEGIN CERTIFICATE-----".to_vec(),
            key: b"-----BEGIN PRIVATE KEY-----".to_vec(),
        }
    }

    #[tokio::test]
    async fn the_production_configuration_runs_in_process() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(service(broker(deployment()))).await?;
        let reading = Telemetry {
            device: "dev42".to_owned(),
            temperature: 21.5,
        };

        tb.broker::<MqttBroker>()
            .message(&reading)
            .to("devices/dev42/telemetry")
            .publish()
            .await?;

        tb.broker::<MqttBroker>()
            .subscriber("devices/+/telemetry")
            .assert_called_once()
            .with(&reading)
            .settled(HandlerOutcome::ack());
        tb.shutdown().await?;
        Ok(())
    }

    #[tokio::test]
    async fn a_keep_alive_below_the_clients_minimum_fails_the_start() {
        let refused = broker(deployment()).keep_alive(Duration::from_secs(1));

        let error = TestApp::start(service(refused))
            .await
            .err()
            .map(|error| error.to_string());

        assert!(
            error
                .as_deref()
                .is_some_and(|error| error.contains("5 seconds")),
            "{error:?}",
        );
    }

    #[tokio::test]
    async fn a_tls_url_without_a_certificate_authority_fails_the_start() {
        let unverifiable = MqttBroker::new("mqtts://mqtt.example.com:8883", "telemetry-1")
            .credentials("telemetry", "s3cret-pa55");

        let error = TestApp::start(service(unverifiable))
            .await
            .err()
            .map(|error| error.to_string());

        assert!(
            error
                .as_deref()
                .is_some_and(|error| error.contains("tls_ca")),
            "{error:?}",
        );
    }
}
