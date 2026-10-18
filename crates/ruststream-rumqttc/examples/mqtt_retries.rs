//! Delayed redelivery on MQTT: what a registration declares about its retries, and where the
//! copies go.
//!
//! MQTT holds no message back and counts no deliveries, so `retry_after` runs on the framework's
//! fallback: the runtime acknowledges the original, waits, and publishes a copy carrying the
//! retry count. Which topic that copy goes to is what the two subscription descriptors differ in.
//! A copy reaches every subscription whose filter matches its topic, so each retried topic and
//! each dead-letter topic here lies outside every other filter the service reads.
//!
//! ```text
//! cargo test --example mqtt_retries --features testing
//! cargo run --example mqtt_retries -- run
//! ```

use std::time::Duration;

// The derive and the value a publish transform reads share the name in different namespaces: the
// prelude carries the macro `ruststream::Outgoing`, and this is the type `runtime::Outgoing`.
use ruststream::runtime::{Names, Outgoing, PublishContext, RETRY_COUNT_HEADER};
use ruststream::schemars::JsonSchema;
use ruststream_rumqttc::prelude::*;
use serde::{Deserialize, Serialize};

/// A meter reading. The type names no topic: each device publishes to its own.
#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing, JsonSchema)]
#[schemars(crate = "ruststream::schemars")]
struct Reading {
    meter: String,
    kwh: f64,
}

/// Stands in for the time-series database the readings are written to. It cannot be reached for
/// meter `m7`, which is what the tests drive.
async fn write(reading: &Reading) -> Result<(), String> {
    tokio::task::yield_now().await;
    if reading.meter == "m7" {
        return Err(format!("meter {} timed out", reading.meter));
    }
    Ok(())
}

/// How long a reading waits before it is written again.
const BACKOFF: Duration = Duration::from_secs(5);

// --8<-- [start:retry_after]
/// One topic is a name a publisher can use, so the copy goes back to this topic and this
/// subscription reads it again, with the retry-count header incremented.
#[subscriber(MqttTopic::new("meters/readings").qos(Qos::AtLeastOnce))]
async fn store(reading: &Reading) -> HandlerOutcome {
    if let Err(err) = write(reading).await {
        tracing::warn!(reading.meter, %err, "write failed, retrying");
        return HandlerOutcome::retry_after(BACKOFF);
    }
    HandlerOutcome::ack()
}
// --8<-- [end:retry_after]

// --8<-- [start:dead_letter]
/// The dead-letter topic is read by an ordinary subscription. The retry-count header says how
/// many times the reading was delivered before it was carried here.
#[subscriber(MqttTopic::new("dead/readings").qos(Qos::AtLeastOnce))]
async fn on_dead_letter(reading: &Reading, ctx: &mut Context) -> HandlerOutcome {
    let deliveries = ctx.headers().get_str(RETRY_COUNT_HEADER).unwrap_or("1");
    tracing::error!(reading.meter, deliveries, "reading dead-lettered");
    HandlerOutcome::ack()
}
// --8<-- [end:dead_letter]

// --8<-- [start:named]
/// A filter over the whole fleet, shared between the instances of the service. A filter is no
/// name a publisher can use, so the mount site names the topic its copies go to.
#[subscriber(MqttFilter::new("devices/+/telemetry").qos(Qos::AtLeastOnce).shared("workers"))]
async fn collect(reading: &Reading) -> HandlerOutcome {
    if let Err(err) = write(reading).await {
        tracing::warn!(reading.meter, %err, "write failed, retrying");
        return HandlerOutcome::retry_after(BACKOFF);
    }
    HandlerOutcome::ack()
}
// --8<-- [end:named]

// --8<-- [start:naming_transform]
/// Sends every copy back to the topic its delivery arrived on, the one topic of the filter's many
/// that this message belongs to. A transform on the retry position reads the delivery being
/// retried, and the topic lives in the broker's per-delivery context.
struct ToDeliveryTopic;

impl<Options> PublishTransform<ForReply<MqttContext>, Options> for ToDeliveryTopic {
    type Destination = Names;

    fn apply(
        &self,
        out: &mut Outgoing<'_>,
        _options: &mut Option<Options>,
        cx: &PublishContext<'_, MqttContext>,
    ) {
        out.set_name(cx.context(DeliveryTopic).to_owned());
    }
}

/// The `Ctx<DeliveryTopic>` parameter hands the body the topic and makes the crate's context the
/// registration's, which is what the transform reads.
#[subscriber(MqttFilter::new("sites/+/telemetry").qos(Qos::AtLeastOnce))]
async fn track(reading: &Reading, Ctx(topic): Ctx<DeliveryTopic>) -> HandlerOutcome {
    if let Err(err) = write(reading).await {
        tracing::warn!(reading.meter, %topic, %err, "write failed, retrying");
        return HandlerOutcome::retry_after(BACKOFF);
    }
    HandlerOutcome::ack()
}
// --8<-- [end:naming_transform]

// --8<-- [start:declaration]
#[ruststream::app]
fn app() -> impl App {
    RustStream::new(AppInfo::new("telemetry-retries", "0.1.0")).with_broker(
        MqttBroker::new("mqtt://localhost:1883", "telemetry-retries"),
        |b| {
            // Five deliveries of one reading, the first included; the fifth failure goes to
            // "dead/readings" instead, payload and headers as they arrived.
            b.include(store)
                .max_attempts(nonzero!(5u32))
                .dead_letter("dead/readings");
            b.include(on_dead_letter);
            // A filter names where its copies go: here one topic the filter matches, so they
            // come back to this subscription.
            b.include(collect)
                .max_attempts(nonzero!(5u32))
                .dead_letter("dead/telemetry")
                .out_retry(Publish::default())
                .to("devices/retry/telemetry");
            // Or per delivery: each copy returns to the topic it arrived on.
            b.include(track)
                .max_attempts(nonzero!(5u32))
                .dead_letter("dead/sites")
                .out_retry(Publish::default())
                .transform(ToDeliveryTopic);
        },
    )
}
// --8<-- [end:declaration]

#[cfg(test)]
mod tests {
    use std::error::Error;
    use std::time::Duration;

    use ruststream::runtime::RETRY_COUNT_HEADER;
    use ruststream::testing::{Outcome, TestApp};
    use ruststream_rumqttc::prelude::*;

    use super::{BACKOFF, Reading, app};

    /// The stand's URL, or `None` without one; `RUSTSTREAM_REQUIRE_LIVE` turns the skip into a
    /// failure.
    fn live_url() -> Option<String> {
        match std::env::var("MQTT_TEST_URL") {
            Ok(url) if !url.is_empty() => Some(url),
            _ => {
                assert!(
                    std::env::var_os("RUSTSTREAM_REQUIRE_LIVE").is_none(),
                    "RUSTSTREAM_REQUIRE_LIVE is set, but MQTT_TEST_URL is missing or empty"
                );
                None
            }
        }
    }

    /// The database cannot be reached for this meter.
    fn unreachable() -> Reading {
        Reading {
            meter: "m7".to_owned(),
            kwh: 1.5,
        }
    }

    #[tokio::test(start_paused = true)]
    async fn a_failed_write_comes_back_after_the_backoff() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;
        tb.broker::<MqttBroker>()
            .message(&unreachable())
            .to("meters/readings")
            .publish()
            .await?;
        tb.broker::<MqttBroker>()
            .subscriber("meters/readings")
            .assert_called_once()
            .settled(HandlerOutcome::retry_after(BACKOFF));

        tb.advance(BACKOFF).await?;

        tb.broker::<MqttBroker>()
            .subscriber("meters/readings")
            .assert_called(2)
            .with(&unreachable());
        tb.broker::<MqttBroker>()
            .published::<Reading>("meters/readings")
            .assert_called(2)
            .with_header(RETRY_COUNT_HEADER, "1");
        tb.shutdown().await?;
        Ok(())
    }

    #[tokio::test(start_paused = true)]
    async fn the_fifth_failure_is_dead_lettered_and_read_there() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;
        tb.broker::<MqttBroker>()
            .message(&unreachable())
            .to("meters/readings")
            .publish()
            .await?;
        for _ in 0..4 {
            tb.advance(BACKOFF).await?;
        }

        tb.broker::<MqttBroker>()
            .subscriber("meters/readings")
            .assert_called(5);
        tb.broker::<MqttBroker>()
            .published::<Reading>("dead/readings")
            .assert_called_once()
            .with(&unreachable())
            .with_header(RETRY_COUNT_HEADER, "5");
        tb.broker::<MqttBroker>()
            .subscriber("dead/readings")
            .assert_called_once()
            .settled(HandlerOutcome::ack());
        tb.shutdown().await?;
        Ok(())
    }

    #[tokio::test(start_paused = true)]
    async fn a_written_reading_is_acknowledged_once() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;
        let written = Reading {
            meter: "m1".to_owned(),
            kwh: 0.5,
        };
        tb.broker::<MqttBroker>()
            .message(&written)
            .to("meters/readings")
            .publish()
            .await?;
        tb.advance(BACKOFF).await?;

        assert_eq!(
            tb.broker::<MqttBroker>()
                .subscriber("meters/readings")
                .outcomes(),
            [Outcome::Ack],
        );
        tb.shutdown().await?;
        Ok(())
    }

    #[tokio::test(start_paused = true)]
    async fn a_filter_sends_its_copies_to_the_topic_its_mount_site_names()
    -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;
        tb.broker::<MqttBroker>()
            .message(&unreachable())
            .to("devices/dev7/telemetry")
            .publish()
            .await?;
        tb.advance(BACKOFF).await?;

        tb.broker::<MqttBroker>()
            .published::<Reading>("devices/retry/telemetry")
            .assert_called_once()
            .with_header(RETRY_COUNT_HEADER, "1");
        tb.broker::<MqttBroker>()
            .subscriber("devices/+/telemetry")
            .assert_called(2)
            .with(&unreachable());
        tb.shutdown().await?;
        Ok(())
    }

    #[tokio::test(start_paused = true)]
    async fn a_copy_returns_to_the_topic_its_delivery_arrived_on() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;
        tb.broker::<MqttBroker>()
            .message(&unreachable())
            .to("sites/s7/telemetry")
            .publish()
            .await?;
        tb.advance(BACKOFF).await?;

        tb.broker::<MqttBroker>()
            .published::<Reading>("sites/s7/telemetry")
            .assert_called(2)
            .with_header(RETRY_COUNT_HEADER, "1");
        tb.broker::<MqttBroker>()
            .subscriber("sites/+/telemetry")
            .assert_called(2);
        tb.shutdown().await?;
        Ok(())
    }

    /// A second reader of the meters, which the service above deliberately does not have.
    #[subscriber(MqttFilter::new("meters/#").qos(Qos::AtLeastOnce))]
    async fn audit(reading: &Reading) -> HandlerOutcome {
        tracing::info!(reading.meter, "audited");
        HandlerOutcome::ack()
    }

    /// A copy is an ordinary publish to the topic, so every subscription whose filter matches it
    /// receives the copy, not only the one that retried.
    #[tokio::test(start_paused = true)]
    async fn a_copy_reaches_every_subscription_whose_filter_matches_its_topic()
    -> Result<(), Box<dyn Error>> {
        let app = RustStream::new(AppInfo::new("telemetry-retries", "0.1.0")).with_broker(
            MqttBroker::new("mqtt://localhost:1883", "telemetry-retries"),
            |b| {
                b.include(super::store);
                b.include(audit)
                    .out_retry(Publish::default())
                    .to("meters/audit");
            },
        );
        let tb = TestApp::start(app).await?;
        tb.broker::<MqttBroker>()
            .message(&unreachable())
            .to("meters/readings")
            .publish()
            .await?;
        tb.advance(BACKOFF).await?;

        tb.broker::<MqttBroker>()
            .subscriber("meters/readings")
            .assert_called(2);
        tb.broker::<MqttBroker>()
            .subscriber("meters/#")
            .assert_called(2);
        tb.shutdown().await?;
        Ok(())
    }

    /// Short enough to keep the live leg quick; in process the clock is paused.
    const DELAY: Duration = Duration::from_millis(300);

    /// Defers a reading once and settles its copy.
    #[subscriber(MqttTopic::new("meters/live").qos(Qos::AtMostOnce))]
    async fn defer_once(reading: &Reading, ctx: &mut Context) -> HandlerOutcome {
        if ctx.headers().get_str(RETRY_COUNT_HEADER).is_none() {
            tracing::info!(reading.meter, "deferred");
            return HandlerOutcome::retry_after(DELAY);
        }
        HandlerOutcome::ack()
    }

    fn deferring(url: &str) -> RustStream {
        RustStream::new(AppInfo::new("telemetry-retries", "0.1.0")).with_broker(
            MqttBroker::new(url, format!("telemetry-retries-{}", std::process::id())),
            |b| {
                b.include(defer_once);
            },
        )
    }

    /// At `QoS` 0 a delivery has no acknowledgement, so the first step of the fallback reports
    /// itself unsupported; the copy is published all the same.
    async fn a_delivery_at_qos_0_is_deferred_too(tb: TestApp<()>) -> Result<(), Box<dyn Error>> {
        tb.broker::<MqttBroker>()
            .message(&unreachable())
            .to("meters/live")
            .publish()
            .await?;
        tb.advance(DELAY).await?;

        tb.broker::<MqttBroker>()
            .published::<Reading>("meters/live")
            .assert_called(2)
            .with_header(RETRY_COUNT_HEADER, "1");
        tb.broker::<MqttBroker>()
            .subscriber("meters/live")
            .assert_called(2)
            .settled(HandlerOutcome::ack());
        tb.shutdown().await?;
        Ok(())
    }

    #[tokio::test(start_paused = true)]
    async fn a_delivery_at_qos_0_is_deferred_too_in_process() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(deferring("mqtt://localhost:1883")).await?;
        a_delivery_at_qos_0_is_deferred_too(tb).await
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_delivery_at_qos_0_is_deferred_too_live() -> Result<(), Box<dyn Error>> {
        let Some(url) = live_url() else {
            return Ok(());
        };
        let tb = TestApp::start_live(deferring(&url)).await?;
        a_delivery_at_qos_0_is_deferred_too(tb).await
    }

    /// Never ready: it asks for an immediate retry every time.
    #[subscriber(MqttTopic::new("meters/batch").qos(Qos::AtLeastOnce))]
    async fn retry_now(reading: &Reading) -> HandlerOutcome {
        tracing::info!(reading.meter, "not ready");
        HandlerOutcome::retry()
    }

    /// Under a cap an immediate retry travels as a copy too, because the count lives in the
    /// header the copies carry.
    #[tokio::test(start_paused = true)]
    async fn under_a_cap_an_immediate_retry_is_a_copy() -> Result<(), Box<dyn Error>> {
        let app = RustStream::new(AppInfo::new("telemetry-retries", "0.1.0")).with_broker(
            MqttBroker::new("mqtt://localhost:1883", "telemetry-retries"),
            |b| {
                b.include(retry_now)
                    .max_attempts(nonzero!(3u32))
                    .dead_letter("dead/batch");
            },
        );
        let tb = TestApp::start(app).await?;
        tb.broker::<MqttBroker>()
            .message(&unreachable())
            .to("meters/batch")
            .publish()
            .await?;

        tb.broker::<MqttBroker>()
            .subscriber("meters/batch")
            .assert_called(3);
        tb.broker::<MqttBroker>()
            .published::<Reading>("dead/batch")
            .assert_called_once()
            .with_header(RETRY_COUNT_HEADER, "3");
        tb.shutdown().await?;
        Ok(())
    }
}
