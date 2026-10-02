//! [`MqttContext`]: what a delivery carries beyond its payload and headers.
//!
//! MQTT publishes to a topic and subscribes with a filter, so the two are not the same string
//! whenever a filter carries a wildcard. The subscription name a handler reads with `ctx.name()`
//! is the filter; the topic the message was actually published to is here, under the
//! [`DeliveryTopic`] key.

use ruststream::{BuildContext, ContextField, Field};

use crate::message::MqttMessage;

/// The per-delivery context of this broker.
///
/// Read a field with a key rather than by field access: `ctx.context(DeliveryTopic)` in a handler
/// body, `cx.context(DeliveryTopic)` in a publish transform on the reply or the retry position.
///
/// # Examples
///
/// ```
/// # mod demo {
/// use ruststream_rumqttc::prelude::*;
/// use serde::Deserialize;
///
/// #[derive(Deserialize)]
/// struct Telemetry {
///     temperature: f64,
/// }
///
/// #[subscriber(MqttFilter::new("devices/+/telemetry"))]
/// async fn record(telemetry: &Telemetry, ctx: &mut Context<'_, MqttContext>) -> HandlerOutcome {
///     let device = ctx.context(DeliveryTopic).split('/').nth(1).unwrap_or_default();
///     println!("{device}: {}", telemetry.temperature);
///     HandlerOutcome::ack()
/// }
///
/// #[ruststream::app]
/// fn app() -> impl App {
///     RustStream::new(AppInfo::new("telemetry", "0.1.0")).with_broker(
///         MqttBroker::new("mqtt://localhost:1883", "telemetry-svc"),
///         |b| {
///             b.include(record)
///                 .out_retry(Publish::default())
///                 .to("devices/retry/telemetry");
///         },
///     )
/// }
/// # }
/// # fn main() {}
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct MqttContext {
    topic: String,
}

impl MqttContext {
    /// Builds the context of a delivery that arrived on `topic`.
    #[must_use]
    pub fn new(topic: impl Into<String>) -> Self {
        Self {
            topic: topic.into(),
        }
    }

    /// The concrete topic this delivery was published to, never the filter that matched it.
    #[must_use]
    pub fn topic(&self) -> &str {
        &self.topic
    }
}

/// The key for [`MqttContext::topic`].
///
/// It reads the topic borrowed through `ctx.context(DeliveryTopic)`, and owned through the
/// `Ctx<DeliveryTopic>` extractor, which is also what fixes a handler's context type to
/// [`MqttContext`] without a `Context` parameter.
///
/// # Examples
///
/// ```
/// # #[cfg(feature = "testing")]
/// # mod demo {
/// use std::error::Error;
///
/// use ruststream::testing::TestApp;
/// use ruststream_rumqttc::prelude::*;
/// use serde::{Deserialize, Serialize};
///
/// #[derive(Deserialize, Serialize, Outgoing)]
/// struct Telemetry {
///     temperature: f64,
/// }
///
/// #[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
/// #[outgoing(name = "alerts")]
/// struct Alert {
///     topic: String,
/// }
///
/// #[subscriber(MqttFilter::new("devices/+/telemetry"))]
/// async fn watch(
///     telemetry: &Telemetry,
///     Ctx(topic): Ctx<DeliveryTopic>,
///     Out(alerts): Out<impl Publisher>,
/// ) -> HandlerOutcome {
///     if telemetry.temperature <= 30.0 {
///         return HandlerOutcome::ack();
///     }
///     if alerts.message(&Alert { topic }).publish().await.is_err() {
///         return HandlerOutcome::retry();
///     }
///     HandlerOutcome::ack()
/// }
///
/// pub fn app() -> impl App {
///     RustStream::new(AppInfo::new("telemetry", "0.1.0")).with_broker(
///         MqttBroker::new("mqtt://localhost:1883", "telemetry-svc"),
///         |b| {
///             b.include(watch)
///                 .out(DefaultSlot, Publish::default())
///                 .out_retry(Publish::default())
///                 .to("devices/retry/telemetry")
///                 .build();
///         },
///     )
/// }
///
/// pub async fn an_alert_names_the_topic_not_the_filter() -> Result<(), Box<dyn Error>> {
///     let tb = TestApp::start(app()).await?;
///
///     tb.broker::<MqttBroker>()
///         .message(&Telemetry { temperature: 31.5 })
///         .to("devices/dev42/telemetry")
///         .publish()
///         .await?;
///
///     tb.broker::<MqttBroker>()
///         .published::<Alert>("alerts")
///         .assert_called_once()
///         .with(&Alert { topic: "devices/dev42/telemetry".to_owned() });
///     tb.shutdown().await?;
///     Ok(())
/// }
/// # }
/// # #[cfg(feature = "testing")]
/// # fn main() -> Result<(), Box<dyn std::error::Error>> {
/// #     tokio::runtime::Builder::new_multi_thread()
/// #         .enable_all()
/// #         .build()?
/// #         .block_on(demo::an_alert_names_the_topic_not_the_filter())
/// # }
/// # #[cfg(not(feature = "testing"))]
/// # fn main() {}
/// ```
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct DeliveryTopic;

impl Field<MqttContext> for DeliveryTopic {
    type Value<'a> = &'a str;

    fn get(self, src: &MqttContext) -> &str {
        src.topic()
    }
}

impl ContextField for DeliveryTopic {
    type Context = MqttContext;
    type Value = String;

    fn read(self, src: &MqttContext) -> String {
        src.topic.clone()
    }
}

impl BuildContext<MqttMessage> for MqttContext {
    fn build(msg: &MqttMessage) -> Self {
        Self::new(msg.topic())
    }
}
