//! The service of `examples/mqtt_retries.rs` written without `#[subscriber]`: each handler
//! implements `Handle`, and the cap, the dead-letter topic and the retry position are the same
//! steps of the mount chain.
//!
//! ```text
//! cargo test --example manual_mqtt_retries --features testing
//! cargo run --example manual_mqtt_retries -- run
//! ```

use std::process::ExitCode;
use std::time::Duration;

use ruststream::runtime::cli::run_main;
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
struct Store;

impl Handle<Reading> for Store {
    async fn handle(
        &self,
        reading: &Reading,
        _outs: &(),
        _ctx: &mut Context<'_>,
    ) -> Result<(), HandlerOutcome> {
        if let Err(err) = write(reading).await {
            tracing::warn!(reading.meter, %err, "write failed, retrying");
            return Err(HandlerOutcome::retry_after(BACKOFF));
        }
        Ok(())
    }
}

fn readings() -> MqttTopic {
    MqttTopic::new("meters/readings").qos(Qos::AtLeastOnce)
}
// --8<-- [end:retry_after]

// --8<-- [start:dead_letter]
/// The dead-letter topic is read by an ordinary subscription. The retry-count header says how
/// many times the reading was delivered before it was carried here.
struct OnDeadLetter;

impl Handle<Reading> for OnDeadLetter {
    async fn handle(
        &self,
        reading: &Reading,
        _outs: &(),
        ctx: &mut Context<'_>,
    ) -> Result<(), HandlerOutcome> {
        let deliveries = ctx.headers().get_str(RETRY_COUNT_HEADER).unwrap_or("1");
        tracing::error!(reading.meter, deliveries, "reading dead-lettered");
        Ok(())
    }
}

fn dead_readings() -> MqttTopic {
    MqttTopic::new("dead/readings").qos(Qos::AtLeastOnce)
}
// --8<-- [end:dead_letter]

// --8<-- [start:named]
/// A filter over the whole fleet, shared between the instances of the service. A filter is no
/// name a publisher can use, so the mount site names the topic its copies go to.
struct Collect;

impl Handle<Reading> for Collect {
    async fn handle(
        &self,
        reading: &Reading,
        _outs: &(),
        _ctx: &mut Context<'_>,
    ) -> Result<(), HandlerOutcome> {
        if let Err(err) = write(reading).await {
            tracing::warn!(reading.meter, %err, "write failed, retrying");
            return Err(HandlerOutcome::retry_after(BACKOFF));
        }
        Ok(())
    }
}

fn fleet() -> MqttFilter {
    MqttFilter::new("devices/+/telemetry")
        .qos(Qos::AtLeastOnce)
        .shared("workers")
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

/// A body typed over `MqttContext` reads the topic and makes the crate's context the
/// registration's, which is what the transform reads.
struct Track;

impl Handle<Reading, (), (), MqttContext> for Track {
    async fn handle(
        &self,
        reading: &Reading,
        _outs: &(),
        ctx: &mut Context<'_, MqttContext>,
    ) -> Result<(), HandlerOutcome> {
        if let Err(err) = write(reading).await {
            let topic = ctx.context(DeliveryTopic);
            tracing::warn!(reading.meter, %topic, %err, "write failed, retrying");
            return Err(HandlerOutcome::retry_after(BACKOFF));
        }
        Ok(())
    }
}

fn sites() -> MqttFilter {
    MqttFilter::new("sites/+/telemetry").qos(Qos::AtLeastOnce)
}
// --8<-- [end:naming_transform]

// --8<-- [start:declaration]
fn app() -> impl App {
    RustStream::new(AppInfo::new("telemetry-retries", "0.1.0")).with_broker(
        MqttBroker::new("mqtt://localhost:1883", "telemetry-retries"),
        |b| {
            // Five deliveries of one reading, the first included; the fifth failure goes to
            // "dead/readings" instead, payload and headers as they arrived.
            b.include(
                subscriber("meters/readings", Store)
                    .map_source(|_| readings())
                    .build(),
            )
            .max_attempts(nonzero!(5u32))
            .dead_letter("dead/readings");
            b.include(
                subscriber("dead/readings", OnDeadLetter)
                    .map_source(|_| dead_readings())
                    .build(),
            );
            // A filter names where its copies go: here one topic the filter matches, so they
            // come back to this subscription.
            b.include(
                subscriber("devices/+/telemetry", Collect)
                    .map_source(|_| fleet())
                    .build(),
            )
            .max_attempts(nonzero!(5u32))
            .dead_letter("dead/telemetry")
            .out_retry(Publish::default())
            .to("devices/retry/telemetry");
            // Or per delivery: each copy returns to the topic it arrived on.
            b.include(
                subscriber("sites/+/telemetry", Track)
                    .map_source(|_| sites())
                    .build(),
            )
            .max_attempts(nonzero!(5u32))
            .dead_letter("dead/sites")
            .out_retry(Publish::default())
            .transform(ToDeliveryTopic);
        },
    )
}
// --8<-- [end:declaration]

// What `#[ruststream::app]` writes: the command line on a multi-threaded Tokio runtime.
fn main() -> ExitCode {
    run_main(app)
}

#[cfg(test)]
mod tests {
    use std::error::Error;

    use ruststream::runtime::RETRY_COUNT_HEADER;
    use ruststream::testing::{Outcome, TestApp};
    use ruststream_rumqttc::prelude::*;

    use super::{BACKOFF, Reading, app};

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
}
