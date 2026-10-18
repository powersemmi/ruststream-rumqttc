//! The service of `examples/mqtt_publishing_responders.rs` written without `#[subscriber]`: the responder implements `Handle`, and
//! the same transform rides the reply position.
//!
//! ```text
//! cargo test --example manual_mqtt_publishing_responders --features testing
//! cargo run --example manual_mqtt_publishing_responders -- run
//! ```

// The derive and the value a publish transform reads share the name in different namespaces: the
// prelude carries the macro `ruststream::Outgoing`, and this is the type `runtime::Outgoing`.
use std::future::{Future, ready};
use std::process::ExitCode;

use ruststream::runtime::cli::run_main;
use ruststream::runtime::{Names, Outgoing, PublishContext};
use ruststream::schemars::JsonSchema;
use ruststream_rumqttc::prelude::*;
use serde::{Deserialize, Serialize};

/// Asks one device for its status.
#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing, JsonSchema)]
#[schemars(crate = "ruststream::schemars")]
#[outgoing(name = "devices/dev42/status/get")]
struct StatusRequest {
    detail: bool,
}

/// The answer. The type names no topic: each request says where its answer goes.
#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing, JsonSchema)]
#[schemars(crate = "ruststream::schemars")]
struct Status {
    firmware: String,
    online: bool,
}

// --8<-- [start:responder]
/// Answers on the response topic of the request, which arrives as the `reply-to` header, and
/// copies the request's correlation data so the requester matches the answer to its question.
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

struct Report;

impl Handle<StatusRequest, Status> for Report {
    fn handle(
        &self,
        request: &StatusRequest,
        _outs: &(),
        _ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<Status, HandlerOutcome>> {
        tracing::info!(request.detail, "status requested");
        ready(Ok(Status {
            firmware: "1.4.2".to_owned(),
            online: true,
        }))
    }
}

fn app() -> impl App {
    RustStream::new(AppInfo::new("device-agent", "0.1.0")).with_broker(
        MqttBroker::new("mqtt://localhost:1883", "device-agent-dev42"),
        |b| {
            // A request without a response topic is answered on the topic named here.
            let report = subscriber("devices/dev42/status/get", Report)
                .reply()
                .to("devices/dev42/status")
                .build();
            b.include(report)
                .out_reply(Publish::default())
                .transform(ToResponseTopic);
        },
    )
}
// --8<-- [end:responder]

// What `#[ruststream::app]` writes: the command line on a multi-threaded Tokio runtime.
fn main() -> ExitCode {
    run_main(app)
}

#[cfg(test)]
mod tests {
    use std::error::Error;

    use ruststream::HeaderMap;
    use ruststream::testing::TestApp;
    use ruststream_rumqttc::prelude::*;

    use super::{Status, StatusRequest, app};

    fn status() -> Status {
        Status {
            firmware: "1.4.2".to_owned(),
            online: true,
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn the_answer_goes_to_the_response_topic_with_the_correlation_data()
    -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;
        let mut headers = HeaderMap::new();
        headers.insert("reply-to", "consoles/c1/responses");
        headers.insert("correlation-id", "req-17");
        tb.broker::<MqttBroker>()
            .message(&StatusRequest { detail: false })
            .with_headers(headers)
            .publish()
            .await?;

        tb.broker::<MqttBroker>()
            .published::<Status>("consoles/c1/responses")
            .assert_called_once()
            .with(&status())
            .with_header("correlation-id", "req-17");
        tb.broker::<MqttBroker>()
            .published::<Status>("devices/dev42/status")
            .assert_not_called();
        tb.shutdown().await?;
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_request_without_a_response_topic_is_answered_on_the_mount_site_one()
    -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;
        tb.broker::<MqttBroker>()
            .message(&StatusRequest { detail: true })
            .publish()
            .await?;

        tb.broker::<MqttBroker>()
            .published::<Status>("devices/dev42/status")
            .assert_called_once()
            .with(&status());
        tb.shutdown().await?;
        Ok(())
    }
}
