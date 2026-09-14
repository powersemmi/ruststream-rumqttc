MQTT 5 for `RustStream`: topic filters with wildcards, shared subscriptions, retained
publishes, and headers that ride user properties.

This crate implements the framework's broker contract over [`rumqttc`](https://docs.rs/rumqttc).
Handlers, routers, codecs and middleware come from [`ruststream`](https://docs.rs/ruststream); what
is here is the transport. MQTT 5 is the only version targeted, because two things the framework
relies on exist only there: user properties, which carry headers natively instead of inside an
envelope, and shared subscriptions, which make competing consumers expressible. How a handler is
written, how a router is composed, which codec encodes a payload and what middleware wraps are the
framework's own subjects, at <https://docs.rs/ruststream/latest/ruststream/runtime/index.html> and
<https://docs.rs/ruststream/latest/ruststream/codec/index.html>.

MQTT is a topic bus with no history. There is no log to seek in, no partition, no transaction and
no server-side retry. What a broker keeps is one retained message per topic and the unacknowledged
messages of a persistent session, so every framework feature that needs more than that runs on the
runtime's own fallback, and the section that owns it says so.

The crate owns one task that drives the client's single event loop, demultiplexes packets to
per-subscription streams by topic-filter match, reconnects with a backoff that starts at 100 ms and
doubles to a 5 second ceiling, and resubscribes when the broker reports the session gone. Polling
that loop is what drives keep-alive, acknowledgement and flow control, so a slow handler never
stalls the connection. A refusal the broker will not reconsider (bad credentials, an unacceptable
client id) ends the task instead, and every subscription returns that error.

# A service

One glob covers a routes file: [`prelude`](crate::prelude) carries the framework's own prelude
plus this crate's surface. [`MqttFilter`] subscribes with the protocol's wildcards, and
[`#[ruststream::app]`](https://docs.rs/ruststream/latest/ruststream/attr.app.html) writes the
`main` that runs it.

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
    println!("{}: {}", telemetry.device, telemetry.temperature);
    HandlerOutcome::ack()
}

#[ruststream::app]
fn app() -> impl App {
    RustStream::new(AppInfo::new("telemetry", "0.1.0")).with_broker(
        MqttBroker::new("mqtt://localhost:1883", "telemetry-svc")
            .keep_alive(Duration::from_secs(30))
            .clean_start(false)
            .session_expiry(Duration::from_secs(3600)),
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

`cargo run -- run` starts it. [`MqttBroker::new`] performs no I/O: it records the URL
(`mqtt://host:port` or `mqtts://host:port`) and the client id, and the runtime connects it once,
after the builder has run. `shutdown` consumes the connected broker, so subscribing or publishing
after it does not compile; a publisher handed out earlier outlives the connection and reports
[`MqttError::NotConnected`] instead of succeeding against a dead session.

Runnable programs sit in the crate's `examples/` directory: `mqtt_service`, `mqtt_batches`,
`mqtt_retries`, `mqtt_retained`.

# Subscribing

Two descriptors, one per thing MQTT names. [`MqttTopic`] subscribes to a topic, [`MqttFilter`] to
a topic filter. Each carries its own quality of service and share group, goes inline in
`#[subscriber(..)]`, and takes its value from `.name(..)` at the mount site when it is written bare
(`#[subscriber(MqttFilter)]`). A bare string, `#[subscriber("devices/dev42/telemetry")]`, is a
topic: the by-name form maps to [`MqttTopic`] with the defaults.

| Descriptor | Accepts | Where a retry copy goes |
|---|---|---|
| [`MqttTopic`] | one topic, no wildcard | the descriptor's own topic; the mount site names nothing |
| [`MqttFilter`] | a topic filter, wildcards included | named at the mount site, or per delivery |

Wildcards are the protocol's own: `+` matches exactly one topic level, `#` matches the rest and
may appear only last. They belong to [`MqttFilter`]; handing one to [`MqttTopic`] returns an error
naming the descriptor that takes it, before any I/O, as an invalid filter or a share group
containing `/`, `+` or `#` does. Wildcards are subscribe-only, so a publish to a topic containing
one sends nothing and returns an error. [`MqttMessage::topic`] reports the concrete topic a
delivery arrived on, never the filter that matched it.

[`Qos`] selects the guarantee and defaults to [`Qos::AtLeastOnce`]: `AtMostOnce` is fire and
forget, `AtLeastOnce` is acknowledged with `PUBACK`, `ExactlyOnce` is the four-packet handshake.
`MqttTopic::new("jobs").shared("workers")` subscribes `$share/workers/jobs`, which is how MQTT
expresses competing consumers; the group name belongs to the subscribed filter only, and
`topic()`, `filter()` and the delivered topic stay the plain form. Two members of one group on a
single connection are one subscription to the broker, so the crate hands their deliveries out in
turn. Dropping a subscriber unsubscribes its filter.

A `&[T]` handler takes `.batch(n)` at the mount site, as on any broker. A PUBLISH packet carries
one message, so the crate assembles the batches on the client: a batch closes when it holds the
size the mount site named, or 20 milliseconds after its first delivery, whichever comes first. The
size is the mount site's and the deadline is the crate's, and an idle subscription waits
indefinitely for that first delivery. Nothing at the mount site says which side of the wire filled
a batch, so a batch handler written for another broker mounts here unchanged. Acknowledgement
stays per delivery: a batch abandoned while it fills leaves its messages unacknowledged.

The per-delivery context is [`MqttContext`], with one key, [`DeliveryTopic`]: the topic this
message arrived on. A body reads it with `Ctx(topic): Ctx<DeliveryTopic>` or through
`ctx: &mut Context<'_, MqttContext>`.

There is no position to seek to and no redelivery count on a delivery: the protocol carries a
duplicate flag and no counter, so `start_at(..)` and a broker-supplied attempt number have nothing
to read here.

## Acknowledgement

A message settles when the handler returns, and what the protocol allows depends on the delivery's
own quality of service:

* `QoS` 1 and 2 acknowledge through the protocol.
* `QoS` 0 reports [`AckError::Unsupported`](ruststream::AckError::Unsupported): no acknowledgement
  packet exists for it.
* `nack(requeue = true)` reports `Unsupported` too. MQTT has no negative acknowledgement.
* `nack(requeue = false)` acknowledges: dropping is the protocol's only terminal answer.

When two overlapping filters both match a message the acknowledgement belongs to exactly one
delivery, and the copies report `Unsupported`.

`HandlerOutcome::retry()` asks the broker to redeliver, and on MQTT nothing can ask. The runtime
logs the refused negative acknowledgement and moves on, so the delivery is never acknowledged: at
`QoS` 1 and 2 the message comes back when a persistent session resumes (`clean_start(false)`, a
session expiry long enough to outlive the gap, and a reconnect), never inside the live connection.
At `QoS` 0 it is gone. Read `retry()` here as "leave it for the next session".

`HandlerOutcome::retry_after(delay)` is the outcome that retries within the session, through the
framework's fallback rather than the protocol: the runtime acknowledges the original, waits, then
publishes a copy carrying the retry count in its headers. Acknowledging the original is that
fallback's first step, so at `QoS` 0 the step is refused and the copy is never published, which
drops the message.

## The retry cap and where a copy goes

`max_attempts(n)` is how many deliveries one message gets, the first included, and
`dead_letter(topic)` is where it goes once they run out. MQTT counts no redeliveries of its own, so
the count is the framework's retry-count header, travelling on the copies. Give the dead-letter
topic a name no subscription of the service reads: one that matches a live filter hands the
message straight back. A cap declared without a topic rejects the message instead, which here means
acknowledging it and letting it go.

A registration on [`MqttTopic`] is complete as it stands, because a topic is a name a publisher can
use, share groups included. A registration on [`MqttFilter`] names the destination itself, either
once at the mount site or per delivery through a publish transform, and one that names neither
refuses to start, naming the subscription: the service learns at startup that `retry_after` has
nowhere to go instead of losing every delayed message.

```
# mod demo {
use ruststream::runtime::{Names, Outgoing, PublishContext};
use ruststream_rumqttc::prelude::*;
use serde::Deserialize;

#[derive(Deserialize)]
struct Reading {
    device: String,
    temperature: f64,
}

#[subscriber(MqttTopic::new("devices/dev42/telemetry").qos(Qos::AtLeastOnce))]
async fn store(reading: &Reading) -> HandlerOutcome {
    HandlerOutcome::retry_after(std::time::Duration::from_secs(5))
}

#[subscriber(MqttFilter::new("devices/+/commands").qos(Qos::AtLeastOnce))]
async fn forward(reading: &Reading, Ctx(topic): Ctx<DeliveryTopic>) -> HandlerOutcome {
    println!("{} on {topic}", reading.device);
    HandlerOutcome::ack()
}

/// Sends every copy back to the topic its own delivery arrived on, which is the one topic of the
/// filter's many that this message belongs to.
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

#[ruststream::app]
fn app() -> impl App {
    RustStream::new(AppInfo::new("telemetry-retries", "0.1.0")).with_broker(
        MqttBroker::new("mqtt://localhost:1883", "telemetry-retries"),
        |b| {
            // A topic addresses its own copies, so this declaration is the whole mount site.
            b.include(store)
                .max_attempts(nonzero!(5u32))
                .dead_letter("dead/telemetry");
            // A filter names the destination per delivery instead of once, so a copy returns to
            // the device it came from rather than to one topic for the fleet.
            b.include(forward)
                .max_attempts(nonzero!(5u32))
                .dead_letter("dead/telemetry")
                .out_retry(Publish::default())
                .transform(ToDeliveryTopic);
        },
    )
}
# }
# fn main() {}
```

Naming a topic and composing a naming transform are mutually exclusive: writing both does not
compile. `.out_retry(policy)` is an ordinary slot position, so `.codec(..)`, `.transform(..)` and
`.map_publisher(..)` follow it; the copy carries the delivery's own bytes, so a codec named there
encodes nothing while a transform still runs, which is the one place a service marks a redelivery
as one.

# Publishing

[`MqttPublish`], re-exported by the prelude as `Publish`, is the policy that constructs the
publisher, and it declares the two arguments a PUBLISH packet carries: a quality of service and
the retain flag. It is also this broker's default policy, so a `#[subscriber(.., publish)]`
handler mounted without a policy of its own sends through it, and a reply goes to the topic its own
type declares. A mount site that does name a policy names one per position: `.out_reply(policy)`
for what the handler returns, `.out(marker, policy)` for a slot the body holds, `.out_retry(policy)`
for the copy a deferred retry publishes.

A publish returns once the client session owns the message, not once the broker has confirmed it.
For `QoS` 1 and 2 the session retransmits until the broker acknowledges, across reconnects.

## Per-message arguments

[`MqttPublishOptions`] is the per-message type, and [`MqttPublishSteps`] puts its two fields on the
publish builder itself: `qos(qos)` and `retain(retain)`, in any order. An argument the call does
not name is the one the policy declared. The values reach the client as the protocol fields they
are, so nothing about them is sent as a user property and a subscriber sees a plain message. A
publish with no call site of its own (a reply, the deferred retry copy) takes the policy whole.

A handler body that takes a step is the single place a body names this broker: it imports this
crate's prelude and bounds its slot with `Options = MqttPublishOptions`.

```
# mod demo {
use ruststream::runtime::PublishError;
use ruststream_rumqttc::MqttError;
use ruststream_rumqttc::prelude::*;
use serde::Deserialize;

#[derive(Deserialize)]
struct Telemetry {
    device: String,
    temperature: f64,
}

/// An MQTT state is bytes on the wire rather than an encoded model, so the type carries its own
/// bytes and no codec runs on them; the `{device}` segment becomes a setter the call fills in.
#[derive(Outgoing, Serialized)]
#[outgoing(name = "devices/{device}/state")]
struct DeviceState(Vec<u8>);

#[derive(OutSlot)]
#[publishes(DeviceState)]
struct States;

/// The reading is one of many; the state it puts the device in is what a service joining later
/// must see. So the state is published retained, and only the state.
#[subscriber(MqttFilter::new("devices/+/telemetry").qos(Qos::AtLeastOnce))]
async fn announce_state(
    telemetry: &Telemetry,
    Out(states): Out<impl Publisher<Options = MqttPublishOptions>, States>,
) -> HandlerOutcome {
    let state = if telemetry.temperature > 30.0 { "hot" } else { "ok" };
    if states
        .message(&DeviceState(state.as_bytes().to_vec()))
        .retain(true)
        .to()
        .device(&telemetry.device)
        .publish()
        .await
        .is_err()
    {
        return HandlerOutcome::retry();
    }
    HandlerOutcome::ack()
}

#[ruststream::app]
fn app() -> impl App {
    RustStream::new(AppInfo::new("retained", "0.1.0")).with_broker(
        MqttBroker::new("mqtt://localhost:1883", "retained-example"),
        |b| {
            // The hook runs once the broker is connected, so the announcement cannot race it.
            b.after_startup(
                Publish::default().qos(Qos::AtLeastOnce).retain(true),
                async move |publisher| -> Result<(), PublishError<MqttError>> {
                    publisher
                        .message(&DeviceState(b"online".to_vec()))
                        .to()
                        .device("dev42")
                        .publish()
                        .await
                },
            );
            // The mount site fixes what every publish through the slot takes; the handler's step
            // is what one message changes.
            b.include(announce_state)
                .out(States, Publish::default().qos(Qos::AtLeastOnce))
                .out_retry(Publish::default())
                .to("devices/retry/telemetry")
                .build();
        },
    )
}
# }
# fn main() {}
```

`Publish::default().retain(true)` publishes retained for a whole publisher: the broker keeps the
last message per topic and hands it to each new subscriber on a matching filter, so a service that
starts after a device announced its state still receives it. Retained messages do not reach shared
subscriptions. Publishing an empty payload retained clears the topic.

## Headers, replies and what is absent

Headers are sent as MQTT 5 user properties, so no envelope format is invented and non-Rust peers
see plain MQTT messages. The well-known `content-type`, `reply-to` and `correlation-id` headers
take the matching first-class properties (content type, response topic, correlation data), in both
directions. A message with no headers is published with no properties at all. A publish whose
content type is textual (`application/json`, any `text/` subtype, any `+json` vendor type) carries
the payload format indicator 1 and every other one carries 0, so a peer reads a JSON body as the
UTF-8 it is; the framework fills `content-type` from the codec of the publish position, which is
what makes this follow the codec without anything being declared.

The correlated `RequestReply` call is not implemented, and neither is `TransactionalPublisher`,
`OwnedTransactions` or `Partitioned`: MQTT has no transactions, no partitions and no routing keys,
and ordering is per topic on a connection. A responder is an ordinary handler: the request carries
its response topic in the `reply-to` header, and the handler reads `ctx.headers().reply_to()` and
publishes the answer there through an injected publisher.

# The prelude

[`prelude`](crate::prelude) re-exports the framework's own prelude plus [`MqttBroker`],
[`MqttTopic`], [`MqttFilter`], [`Qos`], [`MqttPublish`] under the name `Publish`,
[`MqttPublishOptions`], [`MqttPublishSteps`], [`MqttContext`] and [`DeliveryTopic`]. Importing it
is a routes file's statement of which broker it runs on, which is why the framework's glob rides
along.

A handler file usually needs none of it: a body bounds its injected publisher with a capability
trait and names no broker type, so it imports `ruststream::prelude::*` alone. The one exception is
a body that adjusts a per-message argument, which names [`MqttPublishOptions`] in its slot bound
and takes a step from [`MqttPublishSteps`]. A file that mixes two brokers imports the prefixed
[`MqttPublish`] from the crate root instead.

# The generated document

With the `asyncapi` feature, which forwards the framework's, this crate fills the `mqtt` protocol
binding (version 0.2.0) of the generated `AsyncAPI` document. The server says it speaks MQTT 5 and
describes the session the client opens:

```json
{
  "bindingVersion": "0.2.0",
  "clientId": "telemetry-svc",
  "cleanSession": false,
  "keepAlive": 30,
  "sessionExpiryInterval": 3600,
  "maximumPacketSize": 1048576,
  "lastWill": { "topic": "devices/svc/status", "qos": 1, "retain": true }
}
```

A subscription reports the quality of service it reads at on its receive operation, and a publish
policy reports both of its arguments on the send operation of an `Out` slot or a dead-letter topic.
A reply has no send operation of its own, so a reply policy contributes nothing there. Every
message reports the MQTT 5 properties it is mapped through. A responder that answers on the
request's own response topic has no fixed reply channel, so the document reports the reply address
as `null` and points a reader at `$message.header#/reply-to`.

Two things are absent on purpose. Credentials never reach a document teams publish and share, so
neither the URL's user information nor `credentials` appears. The last will contributes its topic,
quality of service and retain flag, but not its payload: that is the content of a message rather
than a coordinate. The payload format indicator is absent for a different reason: it follows the
media type of one message, which the codec of the publish position produces, and the document
reports that media type itself in the `contentType` the framework fills.

# Testing

The `testing` feature ships [`MqttTestBroker`](crate::testing::MqttTestBroker), an in-process
transport with no server and no network, described in [`testing`](crate::testing). A routes file
mounts on it as written, both halves of it: the descriptors open subscriptions here and `Publish`
pairs against it, so there is no in-process descriptor and no in-process policy to swap in. The
harness itself is the framework's, documented at
<https://docs.rs/ruststream/latest/ruststream/testing/index.html>.

```
# #[cfg(feature = "testing")]
# mod demo {
use ruststream::testing::TestApp;
use ruststream_rumqttc::prelude::*;
use ruststream_rumqttc::testing::MqttTestBroker;
use serde::{Deserialize, Serialize};

#[derive(Debug, Deserialize, Serialize, Outgoing)]
struct Telemetry {
    device: String,
    temperature: f64,
}

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
#[outgoing(name = "alerts")]
struct Alert {
    device: String,
}

#[subscriber(MqttFilter::new("devices/+/telemetry").qos(Qos::AtLeastOnce).shared("workers"))]
async fn handle(telemetry: &Telemetry, Out(alerts): Out<impl Publisher>) -> HandlerOutcome {
    if telemetry.temperature <= 30.0 {
        return HandlerOutcome::ack();
    }
    let alert = Alert { device: telemetry.device.clone() };
    if alerts.message(&alert).publish().await.is_err() {
        return HandlerOutcome::retry();
    }
    HandlerOutcome::ack()
}

pub async fn a_hot_reading_raises_an_alert() -> Result<(), Box<dyn std::error::Error>> {
    let app = RustStream::new(AppInfo::new("telemetry", "0.1.0"))
        .with_broker(MqttTestBroker::new(), |b| {
            b.include(handle)
                .out(DefaultSlot, Publish::default().qos(Qos::AtLeastOnce))
                .out_retry(Publish::default())
                .to("devices/retry/telemetry")
                .build();
        });
    let tb = TestApp::start(app).await?;

    tb.broker::<MqttTestBroker>()
        .message(&Telemetry { device: "dev42".to_owned(), temperature: 31.5 })
        .to("devices/dev42/telemetry")
        .publish()
        .await?;

    tb.broker::<MqttTestBroker>()
        .published::<Alert>("alerts")
        .assert_called_once()
        .with(&Alert { device: "dev42".to_owned() });
    Ok(())
}
# }
# #[cfg(feature = "testing")]
# fn main() {
#     tokio::runtime::Builder::new_multi_thread()
#         .enable_all()
#         .build()
#         .unwrap()
#         .block_on(demo::a_hot_reading_raises_an_alert())
#         .unwrap();
# }
# #[cfg(not(feature = "testing"))]
# fn main() {}
```

The wildcard resolves here the way it resolves on the wire, so the injection names the topic a
device would publish to and the body sees it under that topic, never under the filter. What the
stand-in leaves out is the protocol itself: the acknowledgement exchange behind an acknowledged
`QoS`, retained messages, and the session that redelivers. Those are what the live suite against
Eclipse Mosquitto covers, gated behind `MQTT_TEST_URL` and run by `just test-brokers`.

# Operations

* Authentication is `credentials(username, password)` on the synchronous builder. TLS is
  `tls_ca(pem)`, which selects TLS whatever the URL scheme says, plus `tls_client_auth(cert, key)`
  for the managed services that require a client certificate.
* The session is `clean_start(false)` plus `session_expiry(duration)`; resuming a persistent
  session is what redelivers unacknowledged messages. `last_will(topic, payload, qos, retain)`
  names the message the broker publishes if that session dies unexpectedly. `keep_alive(duration)`
  has a protocol floor of 5 seconds.
* `max_packet_size(bytes)` defaults to 1 MiB, well above the client's own 10 KiB cap, which kills
  the connection on a larger payload. `receive_maximum(n)` is the protocol's flow control and
  defaults to 1000: it bounds the unacknowledged `QoS` 1/2 deliveries in flight, and with them an
  unread subscriber's queue. `QoS` 0 has no such bound.
* Credentials and the last will's payload stay out of the generated document; the coordinate a
  client connects to is what it reports.
* A URL the connection will reject still produces a document, so a malformed address surfaces at
  `connect` rather than at generation time.

# Cargo features

Both are off by default; the crate's own surface needs neither.

* `testing`: [`MqttTestBroker`](crate::testing::MqttTestBroker) and the framework's `testing`
  feature with it.
* `asyncapi`: the `mqtt` protocol bindings of the generated document, forwarding the framework's
  own `asyncapi` feature.

The framework's codec is a choice of the service, so this crate enables none: pick `json`,
`msgpack` or `cbor` on `ruststream` itself.
