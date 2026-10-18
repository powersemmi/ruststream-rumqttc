//! The service of the Device state guide written without the macros: each handler implements
//! `Handle` and reads its slot from `Outs`, and `main` is written by hand.
//!
//! ```text
//! cargo test --example manual_guide_device_state --features testing
//! MQTT_TEST_URL=mqtt://127.0.0.1:1883 cargo test --example manual_guide_device_state --features testing
//! cargo run --example manual_guide_device_state --features testing -- run
//! ```

use std::process::ExitCode;

use ruststream::runtime::PublishError;
use ruststream::runtime::cli::run_main;
use ruststream::schemars::JsonSchema;
// The error type is named here because the startup hook returns it.
use ruststream_rumqttc::MqttError;
use ruststream_rumqttc::prelude::*;
use serde::{Deserialize, Serialize};

#[cfg(test)]
#[path = "../../guide/live/mod.rs"]
mod live;

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing, JsonSchema)]
#[schemars(crate = "ruststream::schemars")]
struct Heartbeat {
    gateway: String,
    battery_percent: u8,
}

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing, JsonSchema)]
#[schemars(crate = "ruststream::schemars")]
#[outgoing(name = "fleet/decommissioned")]
struct Decommissioned {
    gateway: String,
}

// --8<-- [start:state]
/// The state of one gateway, as plain text a dashboard shows as it is. The type carries its own
/// bytes, so no codec runs on them, and the `{gateway}` level becomes a setter.
#[derive(Outgoing, Serialized)]
#[outgoing(name = "gateways/{gateway}/state")]
struct GatewayState(Vec<u8>);

#[derive(OutSlot)]
#[publishes(GatewayState)]
struct States;

/// Turns each heartbeat into the gateway's state.
struct Track;

impl<Entry> Handle<Heartbeat, (), Outs<(Entry,)>> for Track
where
    Entry: OutEntry<States, Wire: Publisher>,
{
    async fn handle(
        &self,
        heartbeat: &Heartbeat,
        outs: &Outs<(Entry,)>,
        _ctx: &mut Context<'_>,
    ) -> Result<(), HandlerOutcome> {
        let state = if heartbeat.battery_percent < 20 {
            "low-battery"
        } else {
            "ok"
        };
        let announced = outs
            .get(States)
            .message(&GatewayState(state.as_bytes().to_vec()))
            .to()
            .gateway(&heartbeat.gateway)
            .publish()
            .await;
        if let Err(err) = announced {
            tracing::warn!(heartbeat.gateway, %err, "state not announced");
            return Err(HandlerOutcome::retry());
        }
        Ok(())
    }
}

/// An empty retained payload deletes the retained message, so a removed gateway leaves no state.
struct Forget;

impl<Entry> Handle<Decommissioned, (), Outs<(Entry,)>> for Forget
where
    Entry: OutEntry<States, Wire: Publisher>,
{
    async fn handle(
        &self,
        gone: &Decommissioned,
        outs: &Outs<(Entry,)>,
        _ctx: &mut Context<'_>,
    ) -> Result<(), HandlerOutcome> {
        let cleared = outs
            .get(States)
            .message(&GatewayState(Vec::new()))
            .to()
            .gateway(&gone.gateway)
            .publish()
            .await;
        if let Err(err) = cleared {
            tracing::warn!(gone.gateway, %err, "state not cleared");
            return Err(HandlerOutcome::retry());
        }
        Ok(())
    }
}
// --8<-- [end:state]

// --8<-- [start:status]
/// The service's own status, which dashboards read to tell whether it is running.
#[derive(Outgoing, Serialized)]
#[outgoing(name = "services/gateway-monitor/status")]
struct ServiceStatus(Vec<u8>);

/// If the connection ends without a clean disconnect, the server publishes `offline` on the
/// status topic, retained.
fn broker(url: &str, client_id: &str) -> MqttBroker {
    MqttBroker::new(url, client_id).last_will(
        "services/gateway-monitor/status",
        "offline",
        Qos::AtLeastOnce,
        true,
    )
}
// --8<-- [end:status]

// --8<-- [start:app]
fn service(broker: MqttBroker) -> RustStream {
    RustStream::new(AppInfo::new("gateway-monitor", "0.1.0")).with_broker(broker, |b| {
        // Runs once the broker is connected, and replaces the will's `offline` of an earlier run.
        b.after_startup(
            Publish::default().qos(Qos::AtLeastOnce).retain(true),
            async move |publisher| -> Result<(), PublishError<MqttError>> {
                publisher
                    .message(&ServiceStatus(b"online".to_vec()))
                    .publish()
                    .await
            },
        );
        // Every state goes out retained, so a dashboard that starts later reads it at once.
        let retained = Publish::default().qos(Qos::AtLeastOnce).retain(true);
        let heartbeats = MqttFilter::new("gateways/+/heartbeat").qos(Qos::AtLeastOnce);
        b.include(
            subscriber("gateways/+/heartbeat", Track)
                .map_source(|_| heartbeats)
                .build(),
        )
        .out(States, retained)
        .out_retry(Publish::default())
        .to("gateways/retry/heartbeat")
        .build();
        b.include(subscriber("fleet/decommissioned", Forget).build())
            .out(States, retained)
            .build();
    })
}

fn app() -> impl App<State = ()> {
    service(broker("mqtt://localhost:1883", "gateway-monitor"))
}

fn main() -> ExitCode {
    run_main(app)
}
// --8<-- [end:app]

#[cfg(test)]
mod tests {
    use std::error::Error;
    use std::time::Duration;

    use ruststream::testing::TestApp;
    use ruststream_rumqttc::prelude::*;

    use super::{Decommissioned, Heartbeat, app, broker, live, service};

    const STATUS: &str = "services/gateway-monitor/status";

    fn heartbeat(gateway: &str, battery_percent: u8) -> Heartbeat {
        Heartbeat {
            gateway: gateway.to_owned(),
            battery_percent,
        }
    }

    async fn publish_heartbeat(
        tb: &TestApp<()>,
        heartbeat: &Heartbeat,
    ) -> Result<(), Box<dyn Error + Send + Sync>> {
        tb.broker::<MqttBroker>()
            .message(heartbeat)
            .to(format!("gateways/{}/heartbeat", heartbeat.gateway))
            .publish()
            .await?;
        Ok(())
    }

    #[tokio::test]
    async fn a_heartbeat_publishes_the_gateway_state() -> Result<(), Box<dyn Error + Send + Sync>> {
        let tb = TestApp::start(app()).await?;

        publish_heartbeat(&tb, &heartbeat("gw1", 80)).await?;
        publish_heartbeat(&tb, &heartbeat("gw2", 10)).await?;

        tb.broker::<MqttBroker>()
            .published::<()>("gateways/gw1/state")
            .assert_called_once()
            .with_raw(b"ok");
        tb.broker::<MqttBroker>()
            .published::<()>("gateways/gw2/state")
            .assert_called_once()
            .with_raw(b"low-battery");
        tb.shutdown().await?;
        Ok(())
    }

    #[tokio::test]
    async fn a_decommissioned_gateway_has_its_state_cleared()
    -> Result<(), Box<dyn Error + Send + Sync>> {
        let tb = TestApp::start(app()).await?;

        tb.broker::<MqttBroker>()
            .message(&Decommissioned {
                gateway: "gw1".to_owned(),
            })
            .publish()
            .await?;

        tb.broker::<MqttBroker>()
            .published::<()>("gateways/gw1/state")
            .assert_called_once()
            .with_raw(b"");
        tb.shutdown().await?;
        Ok(())
    }

    #[tokio::test]
    async fn the_service_announces_it_is_online() -> Result<(), Box<dyn Error + Send + Sync>> {
        let tb = TestApp::start(app()).await?;
        tb.settle().await?;

        tb.broker::<MqttBroker>()
            .published::<()>(STATUS)
            .assert_called_once()
            .with_raw(b"online");
        tb.shutdown().await?;
        Ok(())
    }

    /// A payload read as the bytes it is.
    #[derive(Deserialized)]
    struct Raw<'a>(&'a [u8]);

    /// The gateway the live test reports on, unique to the run.
    fn gateway() -> String {
        live::client_id("guide-gw")
    }

    fn state_topic() -> String {
        format!("gateways/{}/state", gateway())
    }

    #[subscriber(MqttTopic::new(STATUS))]
    async fn watch_status(status: &Raw<'_>) {
        tracing::info!(bytes = status.0.len(), "status");
    }

    #[subscriber(MqttTopic::new(state_topic()))]
    async fn watch_state(state: &Raw<'_>) {
        tracing::info!(bytes = state.0.len(), "state");
    }

    /// What a dashboard runs: it reads the service's status and the state of one gateway.
    fn dashboard(url: &str) -> RustStream {
        RustStream::new(AppInfo::new("dashboard", "0.1.0")).with_broker(
            MqttBroker::new(url, live::client_id("guide-dashboard")),
            |b| {
                b.include(watch_status);
                b.include(watch_state);
            },
        )
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_dashboard_that_starts_later_reads_the_states_and_then_the_will()
    -> Result<(), Box<dyn Error + Send + Sync>> {
        let Some(url) = live::url() else {
            return Ok(());
        };
        let id = live::client_id("guide-gateway-monitor");

        // The service announces itself, records a state, and stops cleanly: no will.
        let tb = TestApp::start_live(service(broker(&url, &id))).await?;
        publish_heartbeat(&tb, &heartbeat(&gateway(), 10)).await?;
        tb.shutdown().await?;

        // The dashboard starts afterwards and receives both retained messages on subscribe.
        let board = TestApp::start_live(dashboard(&url)).await?;
        board.advance(Duration::from_secs(1)).await?;
        board
            .broker::<MqttBroker>()
            .subscriber(STATUS)
            .assert_called_once()
            .with_raw(b"online");
        board
            .broker::<MqttBroker>()
            .subscriber(&state_topic())
            .assert_called_once()
            .with_raw(b"low-battery");

        // The service crashes, and the server publishes its will.
        let crash_url = url.clone();
        let crash_id = id.clone();
        live::crash(move || service(broker(&crash_url, &crash_id)))?;
        board.advance(Duration::from_secs(1)).await?;
        let statuses = board
            .broker::<MqttBroker>()
            .subscriber(STATUS)
            .received_raw();
        assert!(
            statuses
                .last()
                .is_some_and(|status| status.as_ref() == b"offline"),
            "{statuses:?}"
        );
        board.shutdown().await?;

        // Removing the gateway clears the retained state the test left on the server.
        let tb = TestApp::start_live(service(broker(&url, &id))).await?;
        tb.broker::<MqttBroker>()
            .message(&Decommissioned { gateway: gateway() })
            .publish()
            .await?;
        tb.shutdown().await?;
        Ok(())
    }
}
