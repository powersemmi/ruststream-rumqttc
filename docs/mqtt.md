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
| `Subscribe` | Yes | `MqttTopic` describes a subscription to one topic filter. See [Subscriptions](#subscriptions). |
| Acknowledgement (`ack` / `nack`) | Partial | `QoS` 1 and 2 settle through the protocol. `QoS` 0 and `nack(requeue = true)` return `AckError::Unsupported`. See [Acknowledgement](#acknowledgement). |
| `BatchSubscriber` | On the client | A PUBLISH packet carries one message, so the crate assembles the batches itself, to the size the mount site named. See [Batches](#batches). |
| `TransactionalPublisher` | No | MQTT has no transactions. |
| `OwnedTransactions` | No | MQTT has no transactions. |
| `RequestReply` | No | MQTT 5 has a response-topic property, which the crate maps to the `reply-to` header in both directions; the correlated `request(msg, timeout)` call is not implemented. A responder is an ordinary handler that publishes to `ctx.headers().reply_to()`. See [Headers](#headers). |
| `Partitioned` | No | MQTT has no partitions or routing keys; ordering is per topic on a connection. |
| `Seekable` / `Positioned` | No | The broker stores one retained message per topic and the unacknowledged messages of a persistent session, and nothing else to reposition into. |
| `DescribeServer` | Yes | `MqttBroker` reports its host and the `mqtt` protocol, which the AsyncAPI schema records. |

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

`MqttTopic` describes one subscription: a topic filter, a quality of service and an optional share
group. It goes inline in `#[subscriber(..)]`, and `ruststream_rumqttc::prelude` carries the
framework's own prelude along with this crate's surface, so one glob covers a service file:

```rust
--8<-- "crates/ruststream-rumqttc/examples/mqtt_service.rs:handler"
```

The app names the broker and includes the handler:

```rust
--8<-- "crates/ruststream-rumqttc/examples/mqtt_service.rs:app"
```

Dropping a subscriber unsubscribes its filter.

### Wildcards

Wildcards are the protocol's own: `+` matches exactly one topic level, `#` matches the rest of the
topic and may appear only as the last level. `MqttMessage::topic` reports the concrete topic a
message arrived on, never the filter that matched it, so a handler on `devices/+/telemetry` reads
which device sent the reading. Wildcards are subscribe-only: a publish to a topic containing one
returns an error and sends nothing.

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
expresses competing consumers. The group name belongs to the subscribed filter only: `filter()` and
the topic reported on delivery stay the plain form.

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

A mount site that does name a policy uses `.out(marker, policy)`:
`.out(Reply, Publish::default().qos(Qos::ExactlyOnce))` for what the handler returns, and the same
call under an `Out` slot's own marker for a publisher the body holds. The policy carries the
arguments in both places, so a slot and a reply are written alike.

Which name a file writes follows from the prelude it imports. A handler file imports
`ruststream::prelude::*` and bounds its injected publisher with a capability trait,
`Out<impl Publisher>` or this crate's `Out<impl MqttPublishOptions>`, so the body names no broker
type at all. A routes file imports `ruststream_rumqttc::prelude::*`, where the policy answers to
`Publish`: a mount site then reads the same whichever broker it runs on, and porting a service
changes the import rather than the call. `MqttPublish` stays at the crate root, for a file that
mixes two brokers and has to say which one it means.

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

`MqttPublishOptions` sets, for one message, the two arguments MQTT carries on every PUBLISH packet:

| Step | Overrides |
| --- | --- |
| `with_qos(qos)` | The delivery quality of service of this message. |
| `with_retain(retain)` | Whether the broker keeps this message as the topic's retained one. |

You can take either step on a publisher, in either order, and then continue with the publish as
usual. An argument the call does not name keeps the publisher's policy value:

```rust
--8<-- "crates/ruststream-rumqttc/examples/mqtt_retained.rs:per_publish"
```

The trait is implemented for the live publisher, for the in-process broker's publisher and for the
`Out` slot entry a handler body holds, so the same call works in a handler
(`Out<impl MqttPublishOptions>`), in a startup hook, and under the `TestApp` harness. A slot publish
stays attributed to its slot: `tb.out::<Marker>()` records it like any other.

The step yields a plain publisher, so a publish built on it uses the crate's default codec rather
than the one named at the include site. A slot publish that needs the include site's codec goes
through the slot's own `message(..)` and names the arguments in its headers instead.

The two arguments reach the send path as headers, `mqtt-qos` (the protocol's own `0`, `1`, `2`) and
`mqtt-retain` (`true` or `false`), both exported as `QOS_HEADER` and `RETAIN_HEADER`. The publisher
consumes them, so neither is sent as a user property.

A value outside those vocabularies returns `MqttError::InvalidPublishArgument`, naming the header
and quoting what arrived, and nothing is sent. The in-process broker refuses it on the same terms.

### Retained messages

`Publish::default().retain(true)` publishes retained: the broker keeps the last message per topic
and hands it to each new subscriber on a matching filter. A service that starts after a device
published its state still receives that state. Retained messages do not reach shared subscriptions.

A publisher that retains everything it sends declares the flag once on its policy; a single
announcement takes it per message with `with_retain(true)`. Either way, the scope's `after_startup`
hook runs the publish once the broker is connected:

```rust
--8<-- "crates/ruststream-rumqttc/examples/mqtt_retained.rs:retained"
```

## Headers

Headers are sent as MQTT 5 user properties, so the crate invents no envelope format and non-Rust
peers see plain MQTT messages. The well-known `content-type`, `reply-to` and `correlation-id`
headers take the matching first-class properties instead (content type, response topic, correlation
data), in both directions. A message with no headers is published with no properties at all, and
the publisher consumes the two [per-message argument](#per-message-arguments) headers rather than
sending them, so they do not count as headers for that rule.

A responder is a plain handler: the incoming request carries its response topic in the `reply-to`
header, and the handler reads `ctx.headers().reply_to()` and publishes the answer to that topic
through an injected publisher.

## Testing

The `testing` feature ships `MqttTestBroker`, an in-process broker that runs a service with no
server and no network. Import it from `ruststream_rumqttc::testing`: the prelude a routes file
imports is the mount site's vocabulary and does not carry it. A test mounts the application on the
broker and drives the real handlers, codecs and middleware through the framework's `TestApp`
harness. See
[Unit-testing a service with TestApp](https://powersemmi.github.io/ruststream/latest/guides/testing/#unit-testing-a-service-with-testapp).

It fills batches the way the real subscriber does, with the same size from the mount site and the
same deadline, so a batch handler receives under the harness what a server would have produced.

It routes by exact topic match, and the protocol itself is not reproduced: quality of service
handshakes, shared group distribution, session redelivery, retained messages and wildcard matching
need a real broker.
