//! The service of the Retries guide: firmware updates that a device may refuse for a while, a cap
//! on the attempts, a dead-letter topic, and the reports of every device retried on their own
//! topics.
//!
//! ```text
//! cargo test --example guide_retries --features testing
//! cargo run --example guide_retries --features testing -- run
//! ```

use std::time::Duration;

// The derive and the value a publish transform reads share the name in different namespaces: the
// prelude carries the macro `ruststream::Outgoing`, and this is the type `runtime::Outgoing`.
use ruststream::runtime::{Names, Outgoing, PublishContext, RETRY_COUNT_HEADER};
use ruststream_rumqttc::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
#[outgoing(name = "updates/install")]
struct Update {
    device: String,
    version: String,
}

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
struct Report {
    device: String,
    uptime_secs: u64,
}

/// Why a device did not take an update.
enum Refusal {
    /// It is running a job and takes no update until the job ends.
    Busy,
    /// The transfer broke off.
    Interrupted,
    /// The image does not fit the device.
    Incompatible,
}

/// Stands in for the call that flashes a device; each test device refuses in its own way.
async fn flash(update: &Update) -> Result<(), Refusal> {
    tokio::task::yield_now().await;
    match update.device.as_str() {
        "busy" => Err(Refusal::Busy),
        "flaky" => Err(Refusal::Interrupted),
        "legacy" => Err(Refusal::Incompatible),
        _ => Ok(()),
    }
}

/// Stands in for the store the reports are written to; it is down for device `dev7`.
async fn store(report: &Report) -> Result<(), String> {
    tokio::task::yield_now().await;
    if report.device == "dev7" {
        return Err(format!("store timed out for {}", report.device));
    }
    Ok(())
}

/// How long a busy device is left alone before the update is offered again.
const BACKOFF: Duration = Duration::from_secs(30);

// --8<-- [start:install]
#[subscriber(MqttTopic::new("updates/install").qos(Qos::AtLeastOnce))]
async fn install(update: &Update) -> HandlerOutcome {
    match flash(update).await {
        Ok(()) => HandlerOutcome::ack(),
        Err(Refusal::Busy) => {
            tracing::info!(update.device, "device busy, offering the update later");
            HandlerOutcome::retry_after(BACKOFF)
        }
        Err(Refusal::Interrupted) => {
            tracing::warn!(update.device, "transfer interrupted, retrying");
            HandlerOutcome::retry()
        }
        Err(Refusal::Incompatible) => {
            tracing::warn!(update.device, update.version, "incompatible image dropped");
            HandlerOutcome::drop()
        }
    }
}
// --8<-- [end:install]

// --8<-- [start:review]
/// The dead-letter topic is read like any other; the retry-count header says how many times the
/// update was delivered before it was moved there.
#[subscriber(MqttTopic::new("updates/dead").qos(Qos::AtLeastOnce))]
async fn review(update: &Update, ctx: &mut Context) -> HandlerOutcome {
    let deliveries = ctx.headers().get_str(RETRY_COUNT_HEADER).unwrap_or("1");
    tracing::error!(
        update.device,
        update.version,
        deliveries,
        "update needs a person"
    );
    HandlerOutcome::ack()
}
// --8<-- [end:review]

// --8<-- [start:reports]
/// Sends every copy back to the topic its delivery arrived on, the one topic of the filter's many
/// that the report belongs to.
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

/// The `Ctx<DeliveryTopic>` parameter makes the crate's context the registration's, which is
/// what the transform reads.
#[subscriber(MqttFilter::new("devices/+/reports").qos(Qos::AtLeastOnce))]
async fn record(report: &Report, Ctx(topic): Ctx<DeliveryTopic>) -> HandlerOutcome {
    if let Err(err) = store(report).await {
        tracing::warn!(%topic, %err, "report not stored, retrying");
        return HandlerOutcome::retry_after(BACKOFF);
    }
    HandlerOutcome::ack()
}
// --8<-- [end:reports]

// --8<-- [start:app]
#[ruststream::app]
fn app() -> impl App<State = ()> {
    RustStream::new(AppInfo::new("firmware", "0.1.0")).with_broker(
        MqttBroker::new("mqtt://localhost:1883", "firmware-svc"),
        |b| {
            // Four deliveries of one update, the first included; the fourth refusal moves it to
            // "updates/dead", payload and headers as they arrived.
            b.include(install)
                .max_attempts(nonzero!(4u32))
                .dead_letter("updates/dead");
            b.include(review);
            b.include(record)
                .max_attempts(nonzero!(4u32))
                .dead_letter("reports/dead")
                .out_retry(Publish::default())
                .transform(ToDeliveryTopic);
        },
    )
}
// --8<-- [end:app]

#[cfg(test)]
mod tests {
    use std::error::Error;

    use ruststream::runtime::RETRY_COUNT_HEADER;
    use ruststream::testing::{Outcome, TestApp};
    use ruststream_rumqttc::prelude::*;

    use super::{BACKOFF, Report, Update, app};

    fn update(device: &str) -> Update {
        Update {
            device: device.to_owned(),
            version: "2.1.0".to_owned(),
        }
    }

    #[tokio::test(start_paused = true)]
    async fn a_busy_device_is_offered_the_update_again_later() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;
        tb.broker::<MqttBroker>()
            .message(&update("busy"))
            .publish()
            .await?;
        tb.broker::<MqttBroker>()
            .subscriber("updates/install")
            .assert_called_once()
            .settled(HandlerOutcome::retry_after(BACKOFF));

        tb.advance(BACKOFF).await?;

        tb.broker::<MqttBroker>()
            .subscriber("updates/install")
            .assert_called(2)
            .with(&update("busy"));
        tb.broker::<MqttBroker>()
            .published::<Update>("updates/install")
            .assert_called(2)
            .with_header(RETRY_COUNT_HEADER, "1");
        tb.shutdown().await?;
        Ok(())
    }

    #[tokio::test(start_paused = true)]
    async fn an_update_that_keeps_waiting_is_dead_lettered() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;
        tb.broker::<MqttBroker>()
            .message(&update("busy"))
            .publish()
            .await?;
        for _ in 0..3 {
            tb.advance(BACKOFF).await?;
        }

        tb.broker::<MqttBroker>()
            .subscriber("updates/install")
            .assert_called(4);
        tb.broker::<MqttBroker>()
            .published::<Update>("updates/dead")
            .assert_called_once()
            .with(&update("busy"))
            .with_header(RETRY_COUNT_HEADER, "4");
        tb.broker::<MqttBroker>()
            .subscriber("updates/dead")
            .assert_called_once()
            .settled(HandlerOutcome::ack());
        tb.shutdown().await?;
        Ok(())
    }

    #[tokio::test(start_paused = true)]
    async fn under_the_cap_an_immediate_retry_is_a_copy_published_at_once()
    -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;
        tb.broker::<MqttBroker>()
            .message(&update("flaky"))
            .publish()
            .await?;

        assert_eq!(
            tb.broker::<MqttBroker>()
                .subscriber("updates/install")
                .outcomes(),
            [Outcome::Nack, Outcome::Nack, Outcome::Nack, Outcome::Nack],
        );
        tb.broker::<MqttBroker>()
            .published::<Update>("updates/dead")
            .assert_called_once()
            .with_header(RETRY_COUNT_HEADER, "4");
        tb.shutdown().await?;
        Ok(())
    }

    #[tokio::test(start_paused = true)]
    async fn an_incompatible_image_is_dropped_and_not_dead_lettered() -> Result<(), Box<dyn Error>>
    {
        let tb = TestApp::start(app()).await?;
        tb.broker::<MqttBroker>()
            .message(&update("legacy"))
            .publish()
            .await?;

        tb.broker::<MqttBroker>()
            .subscriber("updates/install")
            .assert_called_once()
            .settled(HandlerOutcome::drop());
        tb.broker::<MqttBroker>()
            .published::<Update>("updates/dead")
            .assert_not_called();
        tb.shutdown().await?;
        Ok(())
    }

    #[tokio::test(start_paused = true)]
    async fn a_report_copy_returns_to_its_device_topic() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;
        let report = Report {
            device: "dev7".to_owned(),
            uptime_secs: 3_600,
        };
        tb.broker::<MqttBroker>()
            .message(&report)
            .to("devices/dev7/reports")
            .publish()
            .await?;

        tb.advance(BACKOFF).await?;

        tb.broker::<MqttBroker>()
            .published::<Report>("devices/dev7/reports")
            .assert_called(2)
            .with(&report)
            .with_header(RETRY_COUNT_HEADER, "1");
        tb.broker::<MqttBroker>()
            .subscriber("devices/+/reports")
            .assert_called(2);
        tb.shutdown().await?;
        Ok(())
    }
}
