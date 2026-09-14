# MQTT

`ruststream-rumqttc` runs a RustStream service on MQTT 5, over [`rumqttc`](https://docs.rs/rumqttc).
MQTT is a topic bus with no history. The crate covers topic filters with wildcards, quality of
service, shared subscriptions, retained messages, sessions and last wills, and ships an in-process
broker for tests under its `testing` feature. For framework concepts (writing subscribers, routing,
codecs, middleware), see the [RustStream documentation](https://powersemmi.github.io/ruststream/).

```toml
ruststream = { version = "0.7", features = ["macros"] }
ruststream-rumqttc = "0.7"
serde = { version = "1", features = ["derive"] }
```

## Capabilities

Which of the framework's optional capabilities this broker implements natively, and where
acknowledgement lands:

| Capability | Native | Reason |
| --- | --- | --- |
| `Subscribe` | Yes | `MqttTopic` describes a subscription to one topic and `MqttFilter` one to a topic filter. See [Subscriptions](#subscriptions). |
| Acknowledgement (`ack` / `nack`) | Partial | `QoS` 1 and 2 settle through the protocol. `QoS` 0 and `nack(requeue = true)` return `AckError::Unsupported`. See [Acknowledgement](#acknowledgement). |
| `BatchSubscriber` | On the client | A PUBLISH packet carries one message, so the crate assembles the batches itself, to the size the mount site named. See [Batches](#batches). |
| `TransactionalPublisher` | No | MQTT has no transactions. |
| `OwnedTransactions` | No | MQTT has no transactions. |
| `RequestReply` | No | MQTT 5 has a response-topic property, which the crate maps to the `reply-to` header in both directions; the correlated `request(msg, timeout)` call is not implemented. A responder is an ordinary handler that publishes to `ctx.headers().reply_to()`. See [Headers](#headers). |
| `Partitioned` | No | MQTT has no partitions or routing keys; ordering is per topic on a connection. |
| `Seekable` / `Positioned` | No | The broker stores one retained message per topic and the unacknowledged messages of a persistent session, and nothing else to reposition into. |
| `DescribeServer` | Yes | `MqttBroker` reports the host and port a client connects to, the protocol version, and the session it opens. Credentials stay out of it. See [The generated document](#the-generated-document). |

## The lifecycle

Each state of the broker is its own type:

```text
MqttBroker::new(url, client_id)   configuration only, synchronous, no I/O
  .connect()   ->  ConnectedMqttBroker   the live session; subscriptions and publishers
  .shutdown()             ->             a clean DISCONNECT, terminating the connection task
```

`connect` starts the connection task and returns when the broker's first `CONNACK` arrives, or
returns the refusal the broker sent instead.

`shutdown` consumes the connected broker, so publishing or subscribing after it does not compile.
A publisher handed out earlier outlives the connection, and returns `MqttError::NotConnected` once
it is gone rather than succeeding against a dead session.

Session and transport settings sit on the synchronous builder. `credentials`, `keep_alive`,
`clean_start` and `session_expiry` configure the session, and `last_will` names the message the
broker publishes if that session dies unexpectedly. `max_packet_size` defaults to 1 MiB, above the
client's own 10 KiB cap, and `receive_maximum` sets flow control. `tls_ca` and `tls_client_auth`
cover managed MQTT services that require a client certificate.

## Subscriptions

Two descriptors, one per thing MQTT names. `MqttTopic` subscribes to a topic; `MqttFilter`
subscribes with a topic filter, wildcards included. Each carries a quality of service and an
optional share group, goes inline in `#[subscriber(..)]`, and works with
`ruststream_rumqttc::prelude`, which carries the framework's own prelude along with this crate's
surface, so one glob covers a service file:

```rust
--8<-- "crates/ruststream-rumqttc/examples/mqtt_service.rs:handler"
```

The app names the broker and includes the handler:

```rust
--8<-- "crates/ruststream-rumqttc/examples/mqtt_service.rs:app"
```

A handler whose filter belongs to the deployment rather than to the code writes
`#[subscriber(MqttFilter)]` instead and takes the filter from `.name(..)` at the mount site; the
quality of service and the share group are then the defaults. `#[subscriber(MqttTopic)]` does the
same for a subscription to a single topic.

Which of the two a subscription uses decides one thing beyond the wildcards it accepts: where a
deferred redelivery is published. A topic is a name a publisher can use, so `MqttTopic` says where
a copy reaches its subscription again; a filter is not, so a registration on `MqttFilter` names
that topic at its mount site. [What a handler's outcome does here](#what-a-handlers-outcome-does-here)
spells it out.

Dropping a subscriber unsubscribes its filter.

### Wildcards

Wildcards are the protocol's own: `+` matches exactly one topic level, `#` matches the rest of the
topic and may appear only as the last level. They belong to `MqttFilter`; handing one to
`MqttTopic` returns an error naming the descriptor that takes it, before any I/O.
`MqttMessage::topic` reports the concrete topic a message arrived on, never the filter that matched
it, so a handler on `devices/+/telemetry` reads which device sent the reading. Wildcards are
subscribe-only: a publish to a topic containing one returns an error and sends nothing.

An invalid filter returns an error naming the filter, before any I/O.

### Quality of service

`Qos` selects the delivery guarantee, defaulting to `Qos::AtLeastOnce`:

| Variant | Wire behaviour |
| --- | --- |
| `Qos::AtMostOnce` | Fire and forget. No acknowledgement exists for these deliveries. |
| `Qos::AtLeastOnce` | The delivery is acknowledged with `PUBACK`. |
| `Qos::ExactlyOnce` | The four-packet handshake; the client completes the second leg. |

### Shared subscriptions

`MqttTopic::new("jobs").shared("workers")` subscribes `$share/workers/jobs`. The broker splits
matching messages across the group's members instead of giving each one a copy, which is how MQTT
expresses competing consumers. The group name belongs to the subscribed filter only: `topic()`,
`filter()` and the topic reported on delivery stay the plain form.

Two members of one group on a single connection are a single subscription to the broker, so the
crate hands their deliveries out in turn. A share group name that is empty, or that contains `/`,
`+` or `#`, returns an error before any I/O, as an invalid filter does.

### Batches

A batch handler takes `&[T]`, and the mount site names the size:

```rust
--8<-- "crates/ruststream-rumqttc/examples/mqtt_batches.rs:batches"
```

A PUBLISH packet carries one message, so the crate assembles the batches itself. A batch closes
when it holds the size the mount site named, or 20 milliseconds after its first delivery, whichever
comes first. The size is the mount site's and the deadline is the crate's, and a batch never holds
more than the size it was opened with. An idle subscription waits indefinitely for that first
delivery, so a quiet topic costs nothing.

Nothing at the mount site says whether the crate or the broker filled the batch, so a batch handler
written for another broker mounts here unchanged. Acknowledgement stays per delivery: a batch
abandoned while it fills leaves its messages unacknowledged, and they redeliver when a persistent
session resumes.

## Acknowledgement

Acknowledgement follows the delivery's own quality of service, and a message settles when the
handler returns rather than when it arrives:

- `QoS` 1 and 2 acknowledge through the protocol.
- `QoS` 0 deliveries return `AckError::Unsupported`: the protocol has no acknowledgement packet for
  them.
- `nack(requeue = true)` returns `AckError::Unsupported` as well. MQTT has no negative
  acknowledgement, and an unacknowledged message redelivers when a persistent session resumes.
- `nack(requeue = false)` acknowledges: dropping is the only terminal outcome the protocol offers.

When two overlapping filters both match a message, the acknowledgement belongs to exactly one
delivery and the copies return `AckError::Unsupported`.

### What a handler's outcome does here

`HandlerOutcome::retry()` asks the broker to redeliver, and on MQTT nothing can ask. The runtime
logs the refused negative acknowledgement (`ack / nack failed`) and moves on, so the delivery is
never acknowledged. At `QoS` 1 and 2 the message therefore comes back when a persistent session
resumes - `clean_start(false)`, a session expiry long enough to outlive the gap, and a reconnect -
and never inside the live connection: nothing is retried in the seconds after the handler returns.
At `QoS` 0 there is nothing to redeliver and the message is gone. Read `retry()` here as "leave it
for the next session", not as "try again shortly".

`HandlerOutcome::retry_after(delay)` is the outcome that retries within the session, through the
framework's own fallback rather than the protocol. The runtime acknowledges the original, waits,
then publishes a copy carrying the retry count in its headers. Acknowledging the original is that
fallback's first step, so it needs an acknowledgeable delivery: at `QoS` 0 the step is refused and
the deferred copy is never published, which drops the message.

A handler that keeps asking circulates its message until an operator intervenes, and two steps
right after `include` end that:

```rust
--8<-- "crates/ruststream-rumqttc/examples/mqtt_retries.rs:declaration"
```

`max_attempts(n)` is how many deliveries one message gets, the first included. MQTT counts no
redeliveries of its own, so the count is the framework's retry-count header and it travels on the
copies. `dead_letter(topic)` is where a message goes once the attempts run out; give it a topic no
subscription of the service reads, because one that matches a live filter hands the message
straight back. A cap declared without a topic rejects the message instead, which on MQTT means
acknowledging it and letting it go.

Where the copy is published is the descriptor's answer or the mount site's. `MqttTopic` subscribes
to a topic, so it says where a copy reaches the subscription again - shared groups included, since
the group takes the copy between its members - and the declaration above is the whole mount site.
`MqttFilter` subscribes to many topics and can name none of them, because `+` and `#` are
subscribe-only, so the registration names one:

```rust
--8<-- "crates/ruststream-rumqttc/examples/mqtt_retries.rs:named"
```

A topic the filter matches sends the copy back to the same subscription. A registration on a filter
that names neither a topic nor a publish transform refuses to start, naming the subscription: the
service learns at startup that `retry_after` has nowhere to go, instead of losing every delayed
message to a publish that went nowhere.

The other way names the destination per delivery. A filter reads many topics and every message
belongs to one of them, so a copy that goes back to the topic its own delivery arrived on returns
to that device rather than to one topic chosen for the whole fleet. The topic is on the broker's
per-delivery context, under the `DeliveryTopic` key:

```rust
--8<-- "crates/ruststream-rumqttc/examples/mqtt_retries.rs:naming_transform"
```

The transform reads that context, so the handler names it too: `Ctx<DeliveryTopic>` as above, or a
`ctx: &mut Context<'_, MqttContext>` parameter. The mount site then composes the transform instead
of naming a topic:

```rust
--8<-- "crates/ruststream-rumqttc/examples/mqtt_retries.rs:naming_mount"
```

The two forms are mutually exclusive: a registration names a topic or composes a naming transform,
and doing both does not compile.

`out_retry(policy)` also replaces the publisher the copies leave through, which is otherwise this
broker's default policy. The position is an ordinary slot, so the steps after it are a slot's:
`.codec(..)`, `.transform(..)` and `.map_publisher(..)`. The deferred copy carries the delivery's
own bytes, so a codec named there resolves the position and encodes nothing, while a transform runs
on the copy - the one place a service marks a redelivery as one. A transform there reads the
delivery being retried, the way a reply's does.

`HandlerOutcome::drop()` acknowledges, because dropping is the protocol's only terminal answer.

Back-pressure is the protocol's receive-maximum, which `MqttBroker::receive_maximum` sets: the
broker holds no more than that many unacknowledged `QoS` 1/2 deliveries in flight, which is also
what bounds an unread subscriber's queue. `QoS` 0 has no such bound.

## Reconnection

The crate owns a task that polls the client's single event loop and hands each packet to the
subscriptions whose filters match it. Polling is what drives keep-alive, acknowledgements and flow
control, so subscriptions and publishes are issued from the caller's task instead, and a slow
consumer never stalls keep-alive traffic.

The task reconnects on its own with a backoff that starts at 100 ms and doubles to a 5 second
ceiling. A refusal the broker will not reconsider (bad credentials, an unacceptable client id) ends
the task instead, and every subscription returns that error.

The `session_present` flag in `CONNACK` decides what happens to the subscriptions after a
reconnect. With the session present the filters are still registered and nothing is re-sent; with
the session gone every live subscription is subscribed again.

## Publishing

`MqttPublish` is the policy that constructs the publisher, and it declares two things: a quality of
service and the retain flag. It is also this broker's default policy, so a
`#[subscriber(.., publish)]` handler mounted without a policy of its own sends through it. A reply
goes to the topic its own type declares.

A mount site that does name a policy names one per position. `.out_reply(policy)` carries what the
handler returns, `.out_retry(policy)` the copy a deferred retry publishes, and
`.out(marker, policy)` a publisher the body holds under that slot's own marker. The policy carries
the arguments in all three, so `.out_reply(Publish::default().qos(Qos::ExactlyOnce))` and the slot
beside it are written alike.

Which name a file writes follows from the prelude it imports. A handler file imports
`ruststream::prelude::*` and bounds its injected publisher with a capability trait,
`Out<impl Publisher>`, so the body names no broker type at all. A routes file imports
`ruststream_rumqttc::prelude::*`, where the policy answers to `Publish`: a mount site then reads
the same whichever broker it runs on, and porting a service changes the import rather than the
call. `MqttPublish` stays at the crate root, for a file that mixes two brokers and has to say which
one it means.

A publish returns once the client session owns the message, not once the broker has confirmed it.
For `QoS` 1 and 2 the session retransmits until the broker acknowledges, across reconnects.

An MQTT payload is frequently a value the service already holds in bytes (a state string, a sensor
frame, a protobuf record) rather than a model the framework should encode. A type declared as
serialized sends those bytes exactly as they are, with no codec on the path, and its declaration
still names the topic. A `{placeholder}` in that name becomes a setter the call fills in:

```rust
--8<-- "crates/ruststream-rumqttc/examples/mqtt_retained.rs:state"
```

### Per-message arguments

Both arguments MQTT carries on a PUBLISH packet are steps on the publish, taken in any order:

| Step | Sets for this one message |
| --- | --- |
| `qos(qos)` | The delivery quality of service. |
| `retain(retain)` | Whether the broker keeps the message as the topic's retained one. |

An argument the call does not name is the one the mount site's policy declared, so
`Publish::default().qos(Qos::ExactlyOnce)` is the default for every publish through that publisher
and a step is how one message differs:

```rust
--8<-- "crates/ruststream-rumqttc/examples/mqtt_retained.rs:per_publish"
```

The steps sit on the publish itself rather than on a publisher of their own, so a stepped publish
is an ordinary publish in every other respect: it encodes with the codec its include site named,
and a slot publish stays attributed to its slot, which `tb.out::<Marker>()` records like any other.
They are there wherever a publish is built - in a handler body, in a startup hook, in a test.

The values reach the client as the protocol fields they are, so nothing about them is sent as a
user property and a subscriber sees a plain message. A publish with no call site of its own - a
reply, or the copy the runtime publishes for a deferred retry - takes the policy whole.

A handler body that adjusts one is the single place a body names this broker. It imports
`ruststream_rumqttc::prelude::*` for the steps and bounds its slot with `MqttPublishOptions`, so
the signature says which broker the body is written for:

```rust
--8<-- "crates/ruststream-rumqttc/examples/mqtt_retained.rs:stepped_handler"
```

Its mount site declares the defaults, and the step is what one message changes:

```rust
--8<-- "crates/ruststream-rumqttc/examples/mqtt_retained.rs:stepped_mount"
```

A test names the same type to assert what a publish carried:
`tb.out::<States>().assert_called_once().with_options(&MqttPublishOptions::default().retain(true))`,
and `assert_options_default()` is the assertion that a publish took no step at all.

### Retained messages

`Publish::default().retain(true)` publishes retained: the broker keeps the last message per topic
and hands it to each new subscriber on a matching filter. A service that starts after a device
published its state still receives that state. Retained messages do not reach shared subscriptions.

A publisher that retains everything it sends declares the flag once on its policy; a single
announcement takes it per message with `retain(true)`. Either way, the scope's `after_startup`
hook runs the publish once the broker is connected:

```rust
--8<-- "crates/ruststream-rumqttc/examples/mqtt_retained.rs:retained"
```

## Headers

Headers are sent as MQTT 5 user properties, so the crate invents no envelope format and non-Rust
peers see plain MQTT messages. The well-known `content-type`, `reply-to` and `correlation-id`
headers take the matching first-class properties instead (content type, response topic, correlation
data), in both directions. A message with no headers is published with no properties at all: the
[per-message arguments](#per-message-arguments) are not headers but fields of the PUBLISH packet,
resolved over the policy's defaults before the packet is built.

The media type decides one more property of the packet. A publish whose content type is textual -
`application/json`, any `text/` subtype, any `+json` vendor type - carries the payload format
indicator 1, and every other one carries 0, so a non-Rust peer reads a JSON body as the UTF-8 it
is. The framework fills the `content-type` header from the codec of the publish position, which is
what makes this follow the codec without anything being declared.

A responder is a plain handler: the incoming request carries its response topic in the `reply-to`
header, and the handler reads `ctx.headers().reply_to()` and publishes the answer to that topic
through an injected publisher.

## The generated document

The framework generates an AsyncAPI document from the service's own declarations, and this crate
fills what only MQTT knows. Turn it on with the `asyncapi` feature, which forwards the framework's:

```toml
ruststream-rumqttc = { version = "0.7", features = ["asyncapi"] }
```

The server says it speaks MQTT 5 and describes the session the client opens:

```json
--8<-- "crates/ruststream-rumqttc/tests/bindings/server.json"
```

Two things are missing on purpose. Credentials never reach a document teams publish and share, so
neither the URL's user information nor `credentials` appears. The last will contributes its topic,
its quality of service and its retain flag, but not its payload: that is the content of a message
rather than a coordinate, and it may say something internal.

A subscription reports the quality of service it reads at, on its receive operation:

```json
--8<-- "crates/ruststream-rumqttc/tests/bindings/receive_operation.json"
```

A publish policy reports both arguments its packets carry, on the send operation of an `Out` slot
or a dead-letter topic. A reply has no send operation of its own, so a reply policy contributes
nothing there:

```json
--8<-- "crates/ruststream-rumqttc/tests/bindings/send_operation.json"
```

Every message reports the MQTT 5 properties this crate maps it through. The payload format
indicator is not among them: it follows the media type of one message, which the codec of the
publish position produces, and a descriptor or a policy is never handed that codec - the document
reports the media type itself, in the `contentType` the framework fills:

```json
--8<-- "crates/ruststream-rumqttc/tests/bindings/message.json"
```

The messages a service publishes report the correlation data alone. The response topic is a
property a requester sets on its own request, and a reply is not a request, so the binding
describes none:

```json
--8<-- "crates/ruststream-rumqttc/tests/bindings/outgoing_message.json"
```

A responder that answers on the request's own response topic has no fixed reply channel, so the
document reports the reply address as `null` and points a reader at `$message.header#/reply-to`,
the header the response topic arrives in.

## Testing

The `testing` feature ships `MqttTestBroker`, an in-process broker that runs a service with no
server and no network. Import it from `ruststream_rumqttc::testing`: the prelude a routes file
imports is the mount site's vocabulary and does not carry it. A test mounts the application on the
broker and drives the real handlers, codecs and middleware through the framework's `TestApp`
harness. See the framework's
[`testing` module](https://docs.rs/ruststream/latest/ruststream/testing/index.html).

It fills batches the way the real subscriber does, with the same size from the mount site and the
same deadline, so a batch handler receives under the harness what a server would have produced.

A routes file mounts on it as written, both halves of it. `MqttTopic` and `MqttFilter` open a
subscription on the test broker, so the handler a service ships is the handler the harness mounts - the one at the top
of this page, wildcard, quality of service, shared group and all - and `MqttPublish` pairs against
it, so `b.include(handle).out_reply(Publish::default())` is the same line under both brokers.
There is no in-process descriptor and no in-process policy to swap in; the only thing that changes
is the broker the app is built with.

The test broker routes by topic-filter match, the rule the connection task demultiplexes
deliveries with, so that filter selects in process the topics it selects on the wire and a device
publishing `devices/dev42/telemetry` reaches the body. The descriptor is validated here as well: a
filter a server would reject fails startup rather than passing its first test.

The rest of the descriptor is honoured as far as an answer is observable without a server. A share
group makes its members compete: four publishes are four deliveries across the group, not one per
member, so a test can assert that work was shared rather than only that it happened. The quality of
service decides whether a delivery can be settled - the lesser of the publish's and the
subscription's, as on the wire - so a `QoS` 0 delivery reports `AckError::Unsupported` here exactly
as it does against Mosquitto, and a handler cannot quietly prove a guarantee nobody asked for.

What the stand-in leaves out is the protocol itself: the acknowledgement exchange behind an
acknowledged `QoS`, retained messages, and the session that redelivers. A `retain` flag resolves
here and stops for the same reason: a test can still assert what a publish asked for, but nothing
in process keeps a last message per topic. A test on this transport therefore says what a handler
received, how it settled, and what it published where; the live suite against Eclipse Mosquitto,
gated behind `MQTT_TEST_URL`, is what says the same answers hold on a wire. Message replay on a persistent session is one of those: a subscriber that
disconnects and returns under the same client id receives what was published to its topic while it
was away, and only the server run proves it.

The framework's contract suites are run against both. The routing suite is in-process only, while
the lifecycle ladder and the batch capability suite run twice, once against the stand-in and once
against Mosquitto. Each scenario in `tests/stand_in_mqtt.rs` is the twin of a live one in
`tests/integration_mqtt.rs`, so a behaviour asserted in process can be traced to the server run
that backs it.

Settlement answers here what it answers on a wire, down to the refusals. `nack(requeue = true)`
reports `AckError::Unsupported` in process exactly as the real message does, because MQTT has no
negative acknowledgement: a handler returning `HandlerOutcome::retry()` gets no redelivery under
the harness, and no test on this transport can claim a retry a service never receives. The section
on [what a handler's outcome does here](#what-a-handlers-outcome-does-here) is the whole of it,
both in process and against a server.
