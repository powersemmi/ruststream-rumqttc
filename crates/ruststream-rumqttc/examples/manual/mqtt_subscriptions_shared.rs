//! The service of `examples/mqtt_subscriptions_shared.rs` written without the macros: the share
//! group is a setting of the descriptor the chain mounts through `map_source`.
//!
//! ```text
//! cargo test --example manual_mqtt_subscriptions_shared --features testing
//! cargo run --example manual_mqtt_subscriptions_shared --features testing -- run
//! ```

use std::future::{Future, ready};
use std::process::ExitCode;

use ruststream::runtime::cli::run_main;
use ruststream::schemars::JsonSchema;
use ruststream_rumqttc::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing, JsonSchema)]
#[schemars(crate = "ruststream::schemars")]
struct Telemetry {
    device: String,
    temperature: f64,
}

struct Process;

impl Handle<Telemetry> for Process {
    fn handle(
        &self,
        telemetry: &Telemetry,
        _outs: &(),
        _ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<(), HandlerOutcome>> {
        tracing::info!(device = %telemetry.device, telemetry.temperature, "reading processed");
        ready(Ok(()))
    }
}

// --8<-- [start:descriptor]
/// Every instance of the service joins the group `workers`, so each reading is handled once.
fn fleet() -> MqttFilter {
    MqttFilter::new("devices/+/telemetry").shared("workers")
}
// --8<-- [end:descriptor]

fn app() -> impl App {
    RustStream::new(AppInfo::new("telemetry", "0.1.0")).with_broker(
        MqttBroker::new("mqtt://localhost:1883", "telemetry-svc"),
        |b| {
            b.include(
                subscriber("devices/+/telemetry", Process)
                    .build()
                    .map_source(|_| fleet()),
            )
            .out_retry(Publish::default())
            .to("devices/retry/telemetry");
        },
    )
}

// What `#[ruststream::app]` writes: the command line on a multi-threaded Tokio runtime.
fn main() -> ExitCode {
    run_main(app)
}

#[cfg(test)]
mod tests {
    use std::error::Error;

    use ruststream::testing::TestApp;

    use super::*;

    const FLEET: &str = "devices/+/telemetry";

    fn reading(device: &str) -> Telemetry {
        Telemetry {
            device: device.to_owned(),
            temperature: 21.5,
        }
    }

    /// Two members of the group and a plain subscription on the same filter: the group takes one
    /// copy of each reading between its members, and the plain subscription takes its own.
    #[tokio::test]
    async fn a_group_takes_one_copy_of_each_reading() -> Result<(), Box<dyn Error>> {
        let fleet_app = RustStream::new(AppInfo::new("telemetry", "0.1.0")).with_broker(
            MqttBroker::new("mqtt://localhost:1883", "telemetry-svc"),
            |b| {
                for _ in 0..2 {
                    b.include(subscriber(FLEET, Process).build().map_source(|_| fleet()))
                        .out_retry(Publish::default())
                        .to("devices/retry/telemetry");
                }
                b.include(
                    subscriber(FLEET, Process)
                        .build()
                        .map_source(|_| MqttFilter::new(FLEET)),
                )
                .out_retry(Publish::default())
                .to("devices/retry/telemetry");
            },
        );
        let tb = TestApp::start(fleet_app).await?;

        for device in ["dev1", "dev2", "dev3", "dev4"] {
            tb.broker::<MqttBroker>()
                .message(&reading(device))
                .to(format!("devices/{device}/telemetry"))
                .publish()
                .await?;
        }

        // The three subscriptions share the filter's name: four readings handled by the group,
        // and the same four by the plain subscription.
        tb.broker::<MqttBroker>()
            .subscriber(FLEET)
            .assert_called(8)
            .settled(HandlerOutcome::ack());
        Ok(())
    }
}
