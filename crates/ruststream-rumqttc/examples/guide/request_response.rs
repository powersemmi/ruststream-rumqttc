//! The service of the Request and response guide: a device agent that answers configuration
//! queries on the response topic each query names.
//!
//! ```text
//! cargo test --example guide_request_response --features testing
//! cargo run --example guide_request_response --features testing -- run
//! ```

// The derive and the value a publish transform reads share the name in different namespaces: the
// prelude carries the macro `ruststream::Outgoing`, and this is the type `runtime::Outgoing`.
use ruststream::runtime::{Names, Outgoing, PublishContext};
use ruststream_rumqttc::prelude::*;
use serde::{Deserialize, Serialize};

// --8<-- [start:responder]
/// Asks the device for one setting.
#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
#[outgoing(name = "devices/dev42/config/get")]
struct ConfigQuery {
    key: String,
}

/// The answer. The type names no topic: each query says where its answer goes.
#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
struct ConfigValue {
    key: String,
    value: String,
}

/// Stands in for the device's settings.
fn setting(key: &str) -> Option<&'static str> {
    match key {
        "sample-interval" => Some("30s"),
        "firmware" => Some("2.1.0"),
        _ => None,
    }
}

/// A query without a response topic is answered on the topic the mount site names; a key the
/// device does not have gets no answer.
#[subscriber(
    MqttTopic::new("devices/dev42/config/get"),
    reply("devices/dev42/config")
)]
async fn answer(query: &ConfigQuery) -> Result<ConfigValue, HandlerOutcome> {
    let Some(value) = setting(&query.key) else {
        tracing::warn!(query.key, "unknown setting");
        return Err(HandlerOutcome::drop());
    };
    tracing::info!(query.key, "setting reported");
    Ok(ConfigValue {
        key: query.key.clone(),
        value: value.to_owned(),
    })
}
// --8<-- [end:responder]

// --8<-- [start:transform]
/// Sends the answer to the response topic of the query, which arrives as the `reply-to` header,
/// and copies the query's correlation data, so the requester matches the answer to its question.
struct ToResponseTopic;

impl<C, Options> PublishTransform<ForReply<C>, Options> for ToResponseTopic {
    type Destination = Names;

    fn apply(
        &self,
        out: &mut Outgoing<'_>,
        _options: &mut Option<Options>,
        cx: &PublishContext<'_, C>,
    ) {
        if let Some(topic) = cx.headers().reply_to() {
            out.set_name(topic.to_owned());
        }
        if let Some(id) = cx.headers().correlation_id() {
            out.headers_mut().insert("correlation-id", id.to_owned());
        }
    }
}

#[ruststream::app]
fn app() -> impl App<State = ()> {
    RustStream::new(AppInfo::new("device-agent", "0.1.0")).with_broker(
        MqttBroker::new("mqtt://localhost:1883", "device-agent-dev42"),
        |b| {
            b.include(answer)
                .out_reply(Publish::default())
                .transform(ToResponseTopic);
        },
    )
}
// --8<-- [end:transform]

#[cfg(test)]
mod tests {
    use std::error::Error;

    use ruststream::testing::TestApp;
    use ruststream_rumqttc::prelude::*;

    use super::{ConfigQuery, ConfigValue, app};

    fn query(key: &str) -> ConfigQuery {
        ConfigQuery {
            key: key.to_owned(),
        }
    }

    fn interval() -> ConfigValue {
        ConfigValue {
            key: "sample-interval".to_owned(),
            value: "30s".to_owned(),
        }
    }

    #[tokio::test]
    async fn the_answer_goes_to_the_response_topic_with_the_correlation_data()
    -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;
        let mut headers = HeaderMap::new();
        headers.insert("reply-to", "consoles/c1/responses");
        headers.insert("correlation-id", "q-17");

        tb.broker::<MqttBroker>()
            .message(&query("sample-interval"))
            .with_headers(headers)
            .publish()
            .await?;

        tb.broker::<MqttBroker>()
            .published::<ConfigValue>("consoles/c1/responses")
            .assert_called_once()
            .with(&interval())
            .with_header("correlation-id", "q-17");
        tb.broker::<MqttBroker>()
            .published::<ConfigValue>("devices/dev42/config")
            .assert_not_called();
        tb.shutdown().await?;
        Ok(())
    }

    #[tokio::test]
    async fn a_query_without_a_response_topic_is_answered_on_the_mount_site_one()
    -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;

        tb.broker::<MqttBroker>()
            .message(&query("sample-interval"))
            .publish()
            .await?;

        tb.broker::<MqttBroker>()
            .published::<ConfigValue>("devices/dev42/config")
            .assert_called_once()
            .with(&interval());
        tb.shutdown().await?;
        Ok(())
    }

    #[tokio::test]
    async fn an_unknown_setting_gets_no_answer() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;
        let mut headers = HeaderMap::new();
        headers.insert("reply-to", "consoles/c1/responses");

        tb.broker::<MqttBroker>()
            .message(&query("colour"))
            .with_headers(headers)
            .publish()
            .await?;

        tb.broker::<MqttBroker>()
            .subscriber("devices/dev42/config/get")
            .assert_called_once()
            .settled(HandlerOutcome::drop());
        tb.broker::<MqttBroker>()
            .published::<ConfigValue>("consoles/c1/responses")
            .assert_not_called();
        tb.shutdown().await?;
        Ok(())
    }
}
