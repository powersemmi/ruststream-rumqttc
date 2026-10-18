//! The service of the Delivery guarantees guide written without the macros: each handler
//! implements `Handle`, the batch handler over a slice, and `main` is written by hand.
//!
//! ```text
//! cargo test --example manual_guide_delivery --features testing
//! MQTT_TEST_URL=mqtt://127.0.0.1:1883 cargo test --example manual_guide_delivery --features testing
//! cargo run --example manual_guide_delivery --features testing -- run
//! ```

use std::future::{Future, ready};
use std::process::ExitCode;
use std::time::Duration;

use ruststream::runtime::cli::run_main;
use ruststream::schemars::JsonSchema;
use ruststream_rumqttc::prelude::*;
use serde::{Deserialize, Serialize};

#[cfg(test)]
#[path = "../../../tests/live_broker/mod.rs"]
mod live;

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing, JsonSchema)]
#[schemars(crate = "ruststream::schemars")]
struct Usage {
    meter: String,
    kwh: f64,
}

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing, JsonSchema)]
#[schemars(crate = "ruststream::schemars")]
struct Sample {
    meter: String,
    watts: f64,
}

/// Stands in for the ledger the usage is posted to. It cannot be reached for meter `m7`, which is
/// what the tests drive.
async fn post(usage: &Usage) -> Result<(), String> {
    tokio::task::yield_now().await;
    if usage.meter == "m7" {
        return Err(format!("ledger timed out for meter {}", usage.meter));
    }
    Ok(())
}

// --8<-- [start:bill]
/// Usage is money: the server hands it over in the four-packet handshake, and the service
/// acknowledges it only once it is posted.
struct Bill;

impl Handle<Usage> for Bill {
    async fn handle(
        &self,
        usage: &Usage,
        _outs: &(),
        _ctx: &mut Context<'_>,
    ) -> Result<(), HandlerOutcome> {
        if usage.kwh < 0.0 {
            tracing::warn!(usage.meter, "negative usage dropped");
            return Err(HandlerOutcome::drop());
        }
        if let Err(err) = post(usage).await {
            tracing::warn!(usage.meter, %err, "usage left for the next session");
            return Err(HandlerOutcome::retry());
        }
        tracing::info!(usage.meter, usage.kwh, "usage billed");
        Ok(())
    }
}

fn usage_of_every_meter() -> MqttFilter {
    MqttFilter::new("meters/+/usage").qos(Qos::ExactlyOnce)
}
// --8<-- [end:bill]

// --8<-- [start:chart]
/// Power samples feed a live chart: a lost one is replaced by the next, so they travel at
/// `QoS` 0 and are written in batches.
struct Chart;

impl Handle<[Sample]> for Chart {
    fn handle(
        &self,
        samples: &[Sample],
        _outs: &(),
        _ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<(), Vec<HandlerOutcome>>> {
        tracing::info!(count = samples.len(), "samples charted");
        ready(Ok(()))
    }
}

fn power_of_every_meter() -> MqttFilter {
    MqttFilter::new("meters/+/power").qos(Qos::AtMostOnce)
}
// --8<-- [end:chart]

// --8<-- [start:app]
/// The server keeps the session of this client id for a day after the service disconnects.
fn broker(url: &str, client_id: &str) -> MqttBroker {
    MqttBroker::new(url, client_id)
        .clean_start(false)
        .session_expiry(Duration::from_hours(24))
        .receive_maximum(100)
}

fn service(broker: MqttBroker) -> RustStream {
    RustStream::new(AppInfo::new("billing", "0.1.0")).with_broker(broker, |b| {
        b.include(
            subscriber("meters/+/usage", Bill)
                .map_source(|_| usage_of_every_meter())
                .build(),
        )
        .out_retry(Publish::default())
        .to("meters/retry/usage");
        // At most 64 samples per call; a batch with fewer closes 20 ms after its first sample.
        b.include(
            subscriber("meters/+/power", Chart)
                .batch(nonzero!(64))
                .build()
                .map_source(|_| power_of_every_meter()),
        )
        .out_retry(Publish::default())
        .to("meters/retry/power");
    })
}

fn app() -> impl App<State = ()> {
    service(broker("mqtt://localhost:1883", "billing"))
}

fn main() -> ExitCode {
    run_main(app)
}
// --8<-- [end:app]

#[cfg(test)]
mod tests {
    use std::error::Error;

    use futures::future::join_all;
    use ruststream::testing::TestApp;
    use ruststream_rumqttc::prelude::*;

    use super::{Sample, Usage, app, broker, live, service};

    fn usage(meter: &str, kwh: f64) -> Usage {
        Usage {
            meter: meter.to_owned(),
            kwh,
        }
    }

    async fn publish_usage(tb: &TestApp<()>, usage: &Usage) -> Result<(), Box<dyn Error>> {
        tb.broker::<MqttBroker>()
            .message(usage)
            .to(format!("meters/{}/usage", usage.meter))
            .publish()
            .await?;
        Ok(())
    }

    #[tokio::test]
    async fn posted_usage_is_acknowledged() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;

        publish_usage(&tb, &usage("m1", 1.5)).await?;

        tb.broker::<MqttBroker>()
            .subscriber("meters/+/usage")
            .assert_called_once()
            .with(&usage("m1", 1.5))
            .settled(HandlerOutcome::ack());
        tb.shutdown().await?;
        Ok(())
    }

    #[tokio::test]
    async fn negative_usage_is_dropped() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;

        publish_usage(&tb, &usage("m1", -1.0)).await?;

        tb.broker::<MqttBroker>()
            .subscriber("meters/+/usage")
            .assert_called_once()
            .settled(HandlerOutcome::drop());
        tb.shutdown().await?;
        Ok(())
    }

    #[tokio::test]
    async fn usage_the_ledger_refuses_is_left_for_the_next_session() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;

        publish_usage(&tb, &usage("m7", 1.5)).await?;

        tb.broker::<MqttBroker>()
            .subscriber("meters/+/usage")
            .assert_called_once()
            .settled(HandlerOutcome::retry());
        tb.broker::<MqttBroker>()
            .published::<Usage>("meters/retry/usage")
            .assert_not_called();
        tb.shutdown().await?;
        Ok(())
    }

    #[tokio::test]
    async fn samples_that_arrive_together_share_one_call() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;
        let broker = tb.broker::<MqttBroker>();
        let samples: Vec<Sample> = (1..=3)
            .map(|n| Sample {
                meter: format!("m{n}"),
                watts: 640.0,
            })
            .collect();

        for published in join_all(samples.iter().map(|sample| {
            broker
                .message(sample)
                .to(format!("meters/{}/power", sample.meter))
                .publish()
        }))
        .await
        {
            published?;
        }
        tb.settle().await?;

        tb.broker::<MqttBroker>()
            .subscriber("meters/+/power")
            .assert_called_once()
            .assert_batch_sizes(&[3]);
        tb.shutdown().await?;
        Ok(())
    }

    /// The service crashes, a meter reports while it is down, and the server hands the usage
    /// over when the service returns on the same client id.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn usage_reported_while_the_service_was_down_is_billed_on_return()
    -> Result<(), Box<dyn Error + Send + Sync>> {
        let Some(url) = live::url("MQTT_TEST_URL") else {
            return Ok(());
        };
        let id = live::client_id("guide-delivery");
        let meter = live::client_id("meter");

        let crash_url = url.clone();
        let crash_id = id.clone();
        live::crash(move || service(broker(&crash_url, &crash_id)))?;

        let sender = RustStream::new(AppInfo::new("meter", "0.1.0")).register_broker(
            MqttBroker::new(&url, live::client_id("guide-delivery-meter")),
        );
        let sender = TestApp::start_live(sender).await?;
        let while_down = usage(&meter, 2.5);
        sender
            .broker::<MqttBroker>()
            .message(&while_down)
            .to(format!("meters/{meter}/usage"))
            .publish()
            .await?;
        sender.shutdown().await?;

        // The resumed session delivers what it kept before the usage published now.
        let tb = TestApp::start_live(service(broker(&url, &id))).await?;
        tb.broker::<MqttBroker>()
            .message(&usage(&meter, 0.5))
            .to(format!("meters/{meter}/usage"))
            .publish()
            .await?;
        let billed = tb
            .broker::<MqttBroker>()
            .subscriber("meters/+/usage")
            .received::<Usage>();
        assert_eq!(billed.first(), Some(&while_down), "{billed:?}");
        tb.shutdown().await?;

        // A clean start under the same id ends the session the test left on the server.
        TestApp::start_live(service(MqttBroker::new(&url, &id)))
            .await?
            .shutdown()
            .await?;
        Ok(())
    }
}
