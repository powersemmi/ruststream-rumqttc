MQTT 5 broker for the [RustStream](https://github.com/powersemmi/ruststream) messaging framework,
backed by [`rumqttc`](https://docs.rs/rumqttc): topic filters with wildcards, shared
subscriptions, retained publishes, and headers that ride user properties.

A subscription is a topic or a topic filter the server delivers matching messages to, and a
publish is one PUBLISH packet with a quality of service and a retain flag. The crate owns one task
that drives the client's event loop, hands each packet to the subscriptions whose filters match,
and reconnects when the connection drops.

# A service

A handler is an `async fn` over the decoded payload, mounted on a broker inside the application
object, and the attribute writes the `main`:

```
# mod demo {
use std::time::Duration;

use ruststream_rumqttc::prelude::*;
use serde::Deserialize;

#[derive(Deserialize)]
struct Telemetry {
    device: String,
    temperature: f64,
}

#[subscriber(MqttFilter::new("devices/+/telemetry").qos(Qos::AtLeastOnce).shared("workers"))]
async fn handle(telemetry: &Telemetry) -> HandlerOutcome {
    tracing::info!(telemetry.device, telemetry.temperature, "reading recorded");
    HandlerOutcome::ack()
}

#[ruststream::app]
fn app() -> impl App {
    RustStream::new(AppInfo::new("telemetry", "0.1.0")).with_broker(
        MqttBroker::new("mqtt://localhost:1883", "telemetry-svc")
            .keep_alive(Duration::from_secs(30))
            .clean_start(false)
            .session_expiry(Duration::from_hours(1)),
        |b| {
            // A filter names no topic a publisher can use, so the mount site says where a
            // deferred retry copy goes.
            b.include(handle)
                .out_retry(Publish::default())
                .to("devices/retry/telemetry");
        },
    )
}
# }
# fn main() {}
```

`cargo run -- run` starts it. [`MqttBroker::new`] only records the URL and the client id; the
runtime connects the broker at startup and disconnects it when the service stops.

# Where things are

The [guides][guides] take an MQTT service from installation to production, in this order:
[getting started][g-start], [topics, wildcards and shared subscriptions][g-topics],
[delivery guarantees][g-delivery], [device state][g-state], [publishing commands][g-publishing],
[retries and dead letters][g-retries], [request and response][g-request-response],
[testing][g-testing] and [connecting to production][g-production]. The
[architecture][architecture] pages show how the crate sits between `rumqttc` and the runtime, and
the [benchmarks][benchmarks] what it costs.

Each topic below links to its component page on the site, which shows it in a service; the
item's own documentation keeps the detail.

## Broker

- [MqttBroker][c-broker]: [`MqttBroker`], connected into [`ConnectedMqttBroker`].
- [Security][c-security]: [`MqttBroker::credentials`], [`MqttBroker::tls_ca`] and
  [`MqttBroker::tls_client_auth`].
- [Session][c-session]: [`MqttBroker::clean_start`] and [`MqttBroker::session_expiry`], a session
  the server keeps between connections.
- [last_will][c-last-will]: [`MqttBroker::last_will`], the message the server publishes when the
  connection ends without a clean disconnect.
- [Limits][c-limits]: [`MqttBroker::max_packet_size`] and [`MqttBroker::receive_maximum`].
- [Reconnect][c-reconnect]: the backoff from 100 ms to 5 s, and [`MqttBroker::keep_alive`].
- [Startup][c-startup]: the wait for the server's `CONNACK`, and the refusals that fail it.
- [Shutdown][c-shutdown]: the clean `DISCONNECT`, and [`MqttError::NotConnected`] for a
  publisher used afterwards.
- [Prelude][c-prelude]: [`prelude`], the one glob an MQTT service imports.
- [Errors][c-errors]: [`MqttError`].

## Subscriptions

- [Subscriptions][c-subscriptions]: a bare topic in `#[subscriber]` reads that one topic.
- [MqttTopic][c-mqtt-topic]: [`MqttTopic`], one topic, which answers for its own retry copies.
- [MqttFilter][c-mqtt-filter]: [`MqttFilter`], a topic filter with the `+` and `#` wildcards.
- [Qos][c-qos]: [`Qos`], the quality of service a subscription reads at.
- [shared][c-shared]: [`MqttFilter::shared`] and [`MqttTopic::shared`], one member of a share
  group takes each message.
- [Overlaps][c-overlaps]: overlapping filters on one connection, each receiving a message once.
- [Batches][c-batches]: a slice handler over [`MqttSubscriber`] batches assembled on the client.
- [Lanes][c-lanes]: `workers(n, by_key)`, one lane per [`PARTITION_KEY_HEADER`] value, in order.

## Settlement

- [Acknowledgement][c-settlement]: what an `ack` and a `drop` send on an [`MqttMessage`] at each
  quality of service.
- [retry][c-retry]: a delivery left unacknowledged until a persistent session resumes.
- [retry_after][c-retry-after]: a copy the runtime publishes after a delay, within the session.
- [Retries][c-retries]: `max_attempts`, counted on the framework's retry-count header.
- [Dead letters][c-dead-letters]: `dead_letter`, the topic a spent delivery goes to.
- [out_retry][c-out-retry]: where the retry copies of an [`MqttFilter`] registration go.

## Context

- [MqttContext][c-context]: [`MqttContext`] and its key [`DeliveryTopic`], the topic a message
  was published to.

## Publishing

- [MqttPublish][c-publishing]: [`MqttPublish`], the publish policy, and its live
  [`MqttPublisher`].
- [MqttPublishSteps][c-steps]: [`MqttPublishSteps`] and [`MqttPublishOptions`], the quality of
  service and retain flag of one message.
- [Retained][c-retained]: a message the server keeps on its topic for later subscribers.
- [Headers][c-headers]: headers as MQTT 5 user properties, and the three that take the protocol's
  own properties.
- [Responders][c-responders]: a handler that answers on the response topic of each request.

## Tooling

- [MQTT bindings][c-asyncapi]: the [server][c-asyncapi-server],
  [operations][c-asyncapi-operations], [messages][c-asyncapi-messages] and
  [replies][c-asyncapi-replies] of the generated `AsyncAPI` document (feature `asyncapi`).
- [Testing][c-testing]: the in-process MQTT server
  [`TestApp`](https://docs.rs/ruststream/latest/ruststream/testing/struct.TestApp.html) connects
  the broker to (feature `testing`), and [Live][c-live] runs against a running server.

# Cargo features

Both are off by default and additive. `testing` adds the in-process mode `TestApp::start`
connects the broker through, and `asyncapi` contributes the `mqtt` bindings to the generated
document. The codecs, the macros and the harness itself are features of the core crate.

[guides]: https://powersemmi.github.io/ruststream-rumqttc/latest/
[g-start]: https://powersemmi.github.io/ruststream-rumqttc/latest/guides/getting-started/
[g-topics]: https://powersemmi.github.io/ruststream-rumqttc/latest/guides/topics/
[g-delivery]: https://powersemmi.github.io/ruststream-rumqttc/latest/guides/delivery/
[g-state]: https://powersemmi.github.io/ruststream-rumqttc/latest/guides/device-state/
[g-publishing]: https://powersemmi.github.io/ruststream-rumqttc/latest/guides/publishing/
[g-retries]: https://powersemmi.github.io/ruststream-rumqttc/latest/guides/retries/
[g-request-response]: https://powersemmi.github.io/ruststream-rumqttc/latest/guides/request-response/
[g-testing]: https://powersemmi.github.io/ruststream-rumqttc/latest/guides/testing/
[g-production]: https://powersemmi.github.io/ruststream-rumqttc/latest/guides/production/
[architecture]: https://powersemmi.github.io/ruststream-rumqttc/latest/architecture/
[benchmarks]: https://powersemmi.github.io/ruststream-rumqttc/latest/architecture/benchmarks/
[c-broker]: https://powersemmi.github.io/ruststream-rumqttc/latest/components/broker/
[c-security]: https://powersemmi.github.io/ruststream-rumqttc/latest/components/broker/security/
[c-session]: https://powersemmi.github.io/ruststream-rumqttc/latest/components/broker/session/
[c-last-will]: https://powersemmi.github.io/ruststream-rumqttc/latest/components/broker/last-will/
[c-limits]: https://powersemmi.github.io/ruststream-rumqttc/latest/components/broker/limits/
[c-reconnect]: https://powersemmi.github.io/ruststream-rumqttc/latest/components/broker/reconnect/
[c-startup]: https://powersemmi.github.io/ruststream-rumqttc/latest/components/broker/startup/
[c-shutdown]: https://powersemmi.github.io/ruststream-rumqttc/latest/components/broker/shutdown/
[c-prelude]: https://powersemmi.github.io/ruststream-rumqttc/latest/components/broker/prelude/
[c-errors]: https://powersemmi.github.io/ruststream-rumqttc/latest/components/broker/errors/
[c-subscriptions]: https://powersemmi.github.io/ruststream-rumqttc/latest/components/subscriptions/
[c-mqtt-topic]: https://powersemmi.github.io/ruststream-rumqttc/latest/components/subscriptions/mqtt-topic/
[c-mqtt-filter]: https://powersemmi.github.io/ruststream-rumqttc/latest/components/subscriptions/mqtt-filter/
[c-qos]: https://powersemmi.github.io/ruststream-rumqttc/latest/components/subscriptions/qos/
[c-shared]: https://powersemmi.github.io/ruststream-rumqttc/latest/components/subscriptions/shared/
[c-overlaps]: https://powersemmi.github.io/ruststream-rumqttc/latest/components/subscriptions/overlaps/
[c-batches]: https://powersemmi.github.io/ruststream-rumqttc/latest/components/subscriptions/batches/
[c-lanes]: https://powersemmi.github.io/ruststream-rumqttc/latest/components/subscriptions/lanes/
[c-settlement]: https://powersemmi.github.io/ruststream-rumqttc/latest/components/settlement/
[c-retry]: https://powersemmi.github.io/ruststream-rumqttc/latest/components/settlement/retry/
[c-retry-after]: https://powersemmi.github.io/ruststream-rumqttc/latest/components/settlement/retry-after/
[c-retries]: https://powersemmi.github.io/ruststream-rumqttc/latest/components/settlement/retries/
[c-dead-letters]: https://powersemmi.github.io/ruststream-rumqttc/latest/components/settlement/dead-letters/
[c-out-retry]: https://powersemmi.github.io/ruststream-rumqttc/latest/components/settlement/out-retry/
[c-context]: https://powersemmi.github.io/ruststream-rumqttc/latest/components/context/
[c-publishing]: https://powersemmi.github.io/ruststream-rumqttc/latest/components/publishing/
[c-steps]: https://powersemmi.github.io/ruststream-rumqttc/latest/components/publishing/mqtt-publish-steps/
[c-retained]: https://powersemmi.github.io/ruststream-rumqttc/latest/components/publishing/retained/
[c-headers]: https://powersemmi.github.io/ruststream-rumqttc/latest/components/publishing/headers/
[c-responders]: https://powersemmi.github.io/ruststream-rumqttc/latest/components/publishing/responders/
[c-asyncapi]: https://powersemmi.github.io/ruststream-rumqttc/latest/components/asyncapi/
[c-asyncapi-server]: https://powersemmi.github.io/ruststream-rumqttc/latest/components/asyncapi/server/
[c-asyncapi-operations]: https://powersemmi.github.io/ruststream-rumqttc/latest/components/asyncapi/operations/
[c-asyncapi-messages]: https://powersemmi.github.io/ruststream-rumqttc/latest/components/asyncapi/messages/
[c-asyncapi-replies]: https://powersemmi.github.io/ruststream-rumqttc/latest/components/asyncapi/replies/
[c-testing]: https://powersemmi.github.io/ruststream-rumqttc/latest/components/testing/
[c-live]: https://powersemmi.github.io/ruststream-rumqttc/latest/components/testing/live/
