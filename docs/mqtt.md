# MQTT

`ruststream-rumqttc` is the MQTT 5 broker, built on [`rumqttc`](https://docs.rs/rumqttc). It covers
topic filters with wildcards, quality of service, shared subscriptions, retained messages, sessions
and last wills, and ships an in-process test broker under its `testing` feature. For framework
concepts (writing subscribers, routing, codecs, middleware), see the
[RustStream documentation](https://powersemmi.github.io/ruststream/).

```toml
ruststream = { version = "0.7", features = ["macros"] }
ruststream-rumqttc = "0.7"
serde = { version = "1", features = ["derive"] }
```

## Capabilities

Which of the framework's optional capability traits this broker implements natively, and where
acknowledgement lands:

| Capability | Native | Reason |
| --- | --- | --- |
| `Subscribe` | Yes | `ConnectedMqttBroker` opens a subscription from a topic filter, and `MqttTopic` is the `SubscriptionSource` descriptor. See [Subscriptions](#subscriptions). |
| Acknowledgement (`ack` / `nack`) | Partial | `QoS` 1 and 2 settle through the protocol. `QoS` 0 and `nack(requeue = true)` report `AckError::Unsupported`. See [Acknowledgement](#acknowledgement). |
| `BatchSubscriber` | On the client | A PUBLISH packet carries one message, so there is no batch fetch to hand a page size to. The crate assembles the pages instead, to the size the mount site named. See [Pages](#pages). |
| `TransactionalPublisher` | No | MQTT has no transactions. |
| `OwnedTransactions` | No | MQTT has no transactions. |
| `RequestReply` | No | The protocol carries the pieces (MQTT 5 has a response-topic property, which the crate maps to the `reply-to` header in both directions), but the correlated `request(msg, timeout)` call is not implemented. A responder is written today as an ordinary handler that publishes to `ctx.headers().reply_to()`. See [Headers](#headers). |
| `Partitioned` | No | MQTT has no partitions or routing keys; ordering is per topic on a connection. |
| `Seekable` / `Positioned` | No | The broker keeps no history to reposition into. It stores one retained message per topic and the unacknowledged messages of a persistent session, neither of which is a seekable log. |
| `DescribeServer` | Yes | `MqttBroker` reports its host and the `mqtt` protocol, which is what the AsyncAPI schema records. |

## The lifecycle

The broker is a ladder of consuming transitions, so each state is a distinct type:

```text
MqttBroker::new(url, client_id)   configuration only, synchronous, no I/O
  .connect()   ->  ConnectedMqttBroker   the live session; subscriptions and publishers
  .shutdown()             ->             a clean DISCONNECT, terminating the connection task
```

`new` performs no I/O, so an MQTT service is assembled with the same `#[ruststream::app]` macro as
any other broker: the runtime connects once at startup, before opening subscriptions, and
disconnects at the end. `connect` spawns the connection task and returns when the broker's first
`CONNACK` arrives, or fails with the refusal the broker sent. Because `shutdown` consumes the
connected broker, publishing or subscribing after it does not compile. A publisher handed out
earlier still aliases the connection, and reports `MqttError::NotConnected` once it is gone rather
than succeeding against a dead session.

Session and transport settings sit on the synchronous builder: `credentials`, `keep_alive`,
`clean_start` and `session_expiry` for persistent sessions, `last_will` for the message the broker
publishes if the session dies unexpectedly, `max_packet_size` (1 MiB by default, above the client's
own 10 KiB cap), `receive_maximum` for flow control, and `tls_ca` plus `tls_client_auth` for
managed MQTT services that require a client certificate.

## Subscriptions

`MqttTopic` is the subscription descriptor: one topic filter, a quality of service, and an optional
share group. It implements `SubscriptionSource`, so it sits inline in the `#[subscriber(..)]`
decorator. `ruststream_rumqttc::prelude` carries the framework's own prelude along with this
crate's surface, so one glob covers a service file:

```rust
--8<-- "crates/ruststream-rumqttc/examples/mqtt_service.rs:handler"
```

Wiring the handler onto the broker is identical to any other broker:

```rust
--8<-- "crates/ruststream-rumqttc/examples/mqtt_service.rs:app"
```

### Wildcards

Wildcards are the protocol's own: `+` matches exactly one topic level, `#` matches the rest of the
topic and may appear only as the last level. A message reports the concrete topic it arrived on
through `MqttMessage::topic`, never the filter that matched it, so a handler on
`devices/+/telemetry` can still tell which device sent the reading. Wildcards are subscribe-only; a
publish to a topic containing one is rejected before it reaches the wire.

An invalid filter is rejected before any I/O, with a message naming the filter. The client's own
send path cannot report why a request failed, so validation happens in the descriptor.

### Quality of service

`Qos` selects the delivery guarantee, defaulting to `Qos::AtLeastOnce`:

| Variant | Wire behaviour |
| --- | --- |
| `Qos::AtMostOnce` | Fire and forget. No acknowledgement exists for these deliveries. |
| `Qos::AtLeastOnce` | The delivery is acknowledged with `PUBACK`. |
| `Qos::ExactlyOnce` | The four-packet handshake; the client completes the second leg. |

### Shared subscriptions

`MqttTopic::new("jobs").shared("workers")` subscribes `$share/workers/jobs`. The broker distributes
matching messages across the group's members instead of fanning out a copy to each, which is how
competing consumers are expressed in MQTT. The group name is part of the wire filter only:
`filter()` and the topic reported on delivery stay the plain form.

Two members of one group on a single connection are one wire subscription as far as the broker is
concerned, so the crate round-robins their deliveries locally. Share group names are validated with
the filter: an empty name, or one containing `/`, `+`, or `#`, is an error before any I/O.

### Pages

A handler taking `&[T]` consumes a page, and its mount site names the size:

```rust
--8<-- "crates/ruststream-rumqttc/examples/mqtt_pages.rs:pages"
```

MQTT has no batch fetch to hand that number to - a PUBLISH packet carries one message - so the
crate assembles the pages on the client. A page closes when it holds the size the mount site named,
or 20 milliseconds after its first delivery, whichever comes first; an idle subscription waits
indefinitely for that first delivery, so a quiet topic costs nothing. The size is the mount site's
and the deadline is the crate's, and a page never carries more than the size it was opened with -
which is what the framework's conformance batch suite checks, against a server and the in-process
broker alike.

Nothing at the mount site says which side of the wire filled the page, which is the point: a page
handler written for another broker mounts here unchanged. Settlement stays per delivery, so a page
whose stream is dropped mid-fill leaves its collected messages unacknowledged, to redeliver when a
persistent session resumes.

## Acknowledgement

Acknowledgement follows the quality of service of the delivery, under manual acknowledgement
control, so a message settles when the handler returns rather than when it is received:

- `QoS` 1 and 2 acknowledge through the protocol.
- `QoS` 0 deliveries report `AckError::Unsupported`. There is no acknowledgement packet to send, so
  reporting the absence is the honest answer; returning success would claim a guarantee the
  quality of service does not provide.
- `nack(requeue = true)` reports `AckError::Unsupported` as well. MQTT has no negative
  acknowledgement: an unacknowledged message redelivers when a persistent session resumes, and
  nothing the client sends can hasten it.
- `nack(requeue = false)` acknowledges, because dropping is the only terminal outcome the protocol
  offers.

A fanned-out copy carries no acknowledgement either. When two overlapping filters both match a
message the wire acknowledgement belongs to exactly one delivery, and the copies report
`AckError::Unsupported`.

Delivery back-pressure is the protocol's receive-maximum, set with
`MqttBroker::receive_maximum`: the broker bounds how many unacknowledged `QoS` 1/2 deliveries it may
have in flight, which is also what bounds an unread subscriber's queue. `QoS` 0 has no such bound.

## Reconnection

The client exposes a single event loop that must be polled continuously, because polling is what
drives keep-alive, acknowledgements, and flow control. The crate owns a task that does nothing but
poll it and demultiplex packets into per-subscription streams by topic-filter matching.
Subscriptions and publishes are issued from the caller's task through a cloneable client handle, so
a slow consumer never stalls keep-alive traffic.

The task reconnects on its own with exponential backoff from 100 ms to a 5 second ceiling. The
client underneath retries with no delay at all, forever, including after a fatal refusal, so the
backoff and the decision to stop belong to the task: a refusal the broker will not change its mind
about (bad credentials, an unacceptable client id) ends the task and surfaces on every subscription.

Resubscription is driven by the broker's own `session_present` flag in `CONNACK`, which is the
authoritative statement of whether the subscriptions survived. When the session is present the
filters are still registered and nothing is re-sent; when it is gone every live subscription is
subscribed again. Dropping a subscriber unsubscribes its filter.

## Publishing

A publisher is a policy plus the live connection. `MqttPublish` is pure declaration - a quality of
service and the retain flag - so it is constructed anywhere, in a router, in configuration, at a
mount site, and the runtime pairs it with the connected broker at startup. It is also the broker's
default publish policy, so a `#[subscriber(.., publish("dest"))]` handler mounted without an
explicit publisher sends through it.

Which name a file writes follows from which prelude it imports, and the two do not overlap. A
handler file imports `ruststream::prelude::*` and bounds its injected publisher with a capability
trait - `Out<impl Publisher>`, or this crate's `Out<impl MqttPublishOptions>` - so the body names no
broker type at all. A routes file imports `ruststream_rumqttc::prelude::*`, which carries the
framework's prelude along with this crate's surface and gives each policy its uniform mount-site
name: `MqttPublish` is `Publish` there, so a mount site reads the same whichever broker it runs on
and porting a service changes the import rather than the call. The prefixed name stays at the crate
root, for a file that mixes two brokers and has to say which one it means.

A successful publish means the message is owned by the client session, not that the broker has
confirmed it: for `QoS` 1 and 2 the session's state machine retransmits until acknowledged, across
reconnects.

An MQTT payload is frequently a wire value the service already holds - a state string, a sensor
frame, a protobuf record - rather than a model the framework should encode. Declare it as a
serialized type and the bytes leave exactly as they are, with no codec on the path, while the
declaration still names the topic; a `{placeholder}` in that name becomes a setter the call fills
in:

```rust
--8<-- "crates/ruststream-rumqttc/examples/mqtt_retained.rs:state"
```

### Per-message arguments

`MqttPublishOptions` overrides, for one message, the two arguments MQTT carries on every PUBLISH
packet:

| Step | Overrides |
| --- | --- |
| `with_qos(qos)` | The delivery quality of service of this message. |
| `with_retain(retain)` | Whether the broker keeps this message as the topic's retained one. |

Take either step on a publisher, in either order, then continue with the publish as usual. An
argument the call does not name keeps the publisher's policy value:

```rust
--8<-- "crates/ruststream-rumqttc/examples/mqtt_retained.rs:per_publish"
```

The trait is implemented for the live publisher, for the in-process test broker's publisher, and
for the `Out` slot entry a handler body holds, so the same call works in a handler
(`Out<impl MqttPublishOptions>`), in a startup hook, and under the `TestApp` harness. Resolving on
the entry is what keeps the publish attributed: `tb.out::<Marker>()` records it like any other slot
publish.

The step yields a plain publisher, so a publish built on it resolves the crate's default codec
rather than the one named at the include site. A slot publish that needs the include site's codec
goes through the slot's own `message(..)` and names the arguments in its headers instead.

`Publisher::publish` takes a message and nothing else, so the two arguments reach the send path as
headers - `mqtt-qos` (the protocol's own `0`, `1`, `2`) and `mqtt-retain` (`true` or `false`), both
exported as `QOS_HEADER` and `RETAIN_HEADER`. This is the mechanism the framework names for a
delivery option a broker expresses that way, and it is what lets the step wrap a slot entry rather
than the publisher underneath it. The publisher consumes both, so neither travels as a user
property.

A value outside those vocabularies fails the publish with `MqttError::InvalidPublishArgument`,
naming the header and quoting what arrived, and nothing reaches the wire. Sending the message under
the publisher's own quality of service instead would answer a call that asked for one guarantee
with another and say nothing about it, so the error is the only honest outcome. The in-process test
broker refuses it on the same terms, which is where a service meets the mistake first.

### Retained messages

`Publish::default().retain(true)` publishes retained: the broker keeps the last message per
topic and hands it to each new subscriber on a matching filter, so a device's current state is
available to a service that starts after the state was published. Retained messages are not
delivered to shared subscriptions.

A publisher that retains everything it sends declares the flag once on its policy; a single
announcement takes it per message with `with_retain(true)`. Either way the scope's `after_startup`
hook runs the publish once the broker is connected:

```rust
--8<-- "crates/ruststream-rumqttc/examples/mqtt_retained.rs:retained"
```

## Headers

Headers travel as MQTT 5 user properties, so no envelope format is invented and non-Rust peers see
plain MQTT messages. The well-known `content-type`, `reply-to`, and `correlation-id` headers ride
the matching first-class properties instead (content type, response topic, correlation data), in
both directions. A message with no headers is published without any properties at all, and the two
[per-message argument](#per-message-arguments) headers are consumed by the publisher rather than
sent, so they do not count as headers for that rule.

A responder built on this is a plain handler: an incoming request carries its response topic in the
`reply-to` header, so the handler reads `ctx.headers().reply_to()` and publishes the answer to that
topic through an injected publisher.

## Testing

The `testing` feature ships `MqttTestBroker`: an in-process broker that reproduces the crate's core
routing with no server and no network. It follows the same ladder as the real broker, and its
connected form implements `ruststream::testing::TestableBroker`, so the same broker drives the
`TestApp` harness and the framework's conformance suite; inject traffic with
`broker.inject(OutgoingMessage::new(..))` and assert on published output with the free
`ruststream::testing::expect_published`. See
[Unit-testing a service with TestApp](https://powersemmi.github.io/ruststream/latest/guides/testing/#unit-testing-a-service-with-testapp).

It pages the way the real subscriber does, with the same size from the mount site and the same
deadline, so a page handler is handed under the harness what a server would have produced.

The test broker routes by exact address match and does not simulate protocol behaviour. Quality of
service handshakes, shared group distribution, session redelivery, retained messages, and wildcard
demultiplexing are covered by the live suite against Eclipse Mosquitto instead, gated behind
`MQTT_TEST_URL`.
