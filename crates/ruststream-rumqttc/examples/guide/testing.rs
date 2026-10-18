//! The service the Testing guide tests: freezer temperatures shared between the monitors, an alert
//! for a warm freezer, and a review topic for the readings the log keeps refusing.
//!
//! ```text
//! cargo test --example guide_testing --features testing
//! MQTT_TEST_URL=mqtt://127.0.0.1:1883 cargo test --example guide_testing --features testing
//! ```

use std::time::Duration;

use ruststream_rumqttc::prelude::*;
use serde::{Deserialize, Serialize};

#[cfg(test)]
#[path = "../../tests/live_broker/mod.rs"]
mod live;

// --8<-- [start:service]
/// The warmest a freezer may be, in degrees Celsius.
const LIMIT_CELSIUS: f64 = -18.0;

/// How long a reading waits before the log is tried again.
const RETRY: Duration = Duration::from_secs(10);

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
struct Reading {
    freezer: String,
    celsius: f64,
}

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
#[outgoing(name = "freezers/alerts")]
struct Alert {
    freezer: String,
    celsius: f64,
}

#[derive(OutSlot)]
#[publishes(Alert)]
struct Alerts;

/// Stands in for the temperature log every reading is kept in; it is down for freezer `f7`.
async fn log(reading: &Reading) -> Result<(), String> {
    tokio::task::yield_now().await;
    if reading.freezer == "f7" {
        return Err(format!("log timed out for {}", reading.freezer));
    }
    Ok(())
}

/// Logs every reading and raises an alert for a freezer warmer than the limit.
#[subscriber(MqttFilter::new("freezers/+/temperature").qos(Qos::AtLeastOnce).shared("monitors"))]
async fn watch(reading: &Reading, Out(alerts): Out<impl Publisher, Alerts>) -> HandlerOutcome {
    if let Err(err) = log(reading).await {
        tracing::warn!(reading.freezer, %err, "reading not logged, retrying");
        return HandlerOutcome::retry_after(RETRY);
    }
    if reading.celsius <= LIMIT_CELSIUS {
        return HandlerOutcome::ack();
    }
    let alert = Alert {
        freezer: reading.freezer.clone(),
        celsius: reading.celsius,
    };
    if let Err(err) = alerts.message(&alert).publish().await {
        tracing::warn!(reading.freezer, %err, "alert not raised, retrying");
        return HandlerOutcome::retry_after(RETRY);
    }
    tracing::warn!(reading.freezer, reading.celsius, "freezer too warm");
    HandlerOutcome::ack()
}

fn service(broker: MqttBroker) -> RustStream {
    RustStream::new(AppInfo::new("cold-chain", "0.1.0")).with_broker(broker, |b| {
        b.include(watch)
            .max_attempts(nonzero!(3u32))
            .dead_letter("freezers/review")
            .out(Alerts, Publish::default().qos(Qos::AtLeastOnce))
            .out_retry(Publish::default())
            .to("freezers/retry/temperature")
            .build();
    })
}

#[ruststream::app]
fn app() -> impl App<State = ()> {
    service(MqttBroker::new("mqtt://localhost:1883", "cold-chain"))
}
// --8<-- [end:service]

#[cfg(test)]
mod tests {
    use std::error::Error;

    use ruststream::runtime::RETRY_COUNT_HEADER;
    use ruststream::testing::{Outcome, TestApp};
    use ruststream_rumqttc::prelude::*;

    use super::{Alert, Alerts, RETRY, Reading, live, service};

    fn reading(freezer: &str, celsius: f64) -> Reading {
        Reading {
            freezer: freezer.to_owned(),
            celsius,
        }
    }

    // --8<-- [start:in_process]
    #[tokio::test]
    async fn a_warm_freezer_raises_an_alert() -> Result<(), Box<dyn Error + Send + Sync>> {
        let broker = MqttBroker::new("mqtt://localhost:1883", "cold-chain");
        let tb = TestApp::start(service(broker)).await?;
        a_warm_reading_raises_one_alert(tb).await
    }

    async fn a_warm_reading_raises_one_alert(
        tb: TestApp<()>,
    ) -> Result<(), Box<dyn Error + Send + Sync>> {
        tb.broker::<MqttBroker>()
            .message(&reading("f1", -12.5))
            .to("freezers/f1/temperature")
            .publish()
            .await?;

        tb.broker::<MqttBroker>()
            .subscriber("freezers/+/temperature")
            .assert_called_once()
            .settled(HandlerOutcome::ack());
        tb.broker::<MqttBroker>()
            .published::<Alert>("freezers/alerts")
            .assert_called_once()
            .with(&Alert {
                freezer: "f1".to_owned(),
                celsius: -12.5,
            });
        tb.shutdown().await?;
        Ok(())
    }
    // --8<-- [end:in_process]

    // --8<-- [start:cold]
    #[tokio::test]
    async fn a_cold_freezer_raises_no_alert() -> Result<(), Box<dyn Error + Send + Sync>> {
        let broker = MqttBroker::new("mqtt://localhost:1883", "cold-chain");
        let tb = TestApp::start(service(broker)).await?;

        tb.broker::<MqttBroker>()
            .message(&reading("f1", -21.0))
            .to("freezers/f1/temperature")
            .publish()
            .await?;

        tb.broker::<MqttBroker>()
            .subscriber("freezers/+/temperature")
            .assert_called_once()
            .settled(HandlerOutcome::ack());
        tb.out::<Alerts>().assert_not_called();
        tb.shutdown().await?;
        Ok(())
    }
    // --8<-- [end:cold]

    // --8<-- [start:delayed]
    #[tokio::test(start_paused = true)]
    async fn a_reading_the_log_keeps_refusing_goes_to_review()
    -> Result<(), Box<dyn Error + Send + Sync>> {
        let broker = MqttBroker::new("mqtt://localhost:1883", "cold-chain");
        let tb = TestApp::start(service(broker)).await?;
        let refused = reading("f7", -20.0);

        tb.broker::<MqttBroker>()
            .message(&refused)
            .to("freezers/f7/temperature")
            .publish()
            .await?;
        tb.broker::<MqttBroker>()
            .subscriber("freezers/+/temperature")
            .assert_called_once()
            .settled(HandlerOutcome::retry_after(RETRY));

        // The delay is a copy of the reading, published to the retry topic once it has passed.
        tb.advance(RETRY).await?;
        tb.broker::<MqttBroker>()
            .published::<Reading>("freezers/retry/temperature")
            .assert_called_once()
            .with(&refused)
            .with_header(RETRY_COUNT_HEADER, "1");

        // The third delivery is the last one: it goes to review as it arrived.
        tb.advance(RETRY).await?;
        assert_eq!(
            tb.broker::<MqttBroker>()
                .subscriber("freezers/+/temperature")
                .outcomes(),
            [Outcome::Nack, Outcome::Nack, Outcome::Nack],
        );
        tb.broker::<MqttBroker>()
            .published::<Reading>("freezers/review")
            .assert_called_once()
            .with(&refused);
        tb.shutdown().await?;
        Ok(())
    }
    // --8<-- [end:delayed]

    // --8<-- [start:live]
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_warm_freezer_raises_an_alert_on_mosquitto()
    -> Result<(), Box<dyn Error + Send + Sync>> {
        let Some(url) = live::url("MQTT_TEST_URL") else {
            return Ok(());
        };
        let broker = MqttBroker::new(url, live::client_id("cold-chain"));
        let tb = TestApp::start_live(service(broker)).await?;
        a_warm_reading_raises_one_alert(tb).await
    }
    // --8<-- [end:live]
}
