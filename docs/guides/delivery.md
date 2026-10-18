---
sidebar_position: 3
description: "Choose the MQTT quality of service, see what each handler outcome sends, keep a persistent session across restarts and bound the deliveries in flight."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Delivery guarantees

This guide chooses a quality of service per subscription, shows what each handler outcome sends to the server, and keeps the messages a service misses while it is down.

The example is a billing service.
Meters report their energy usage, and every report is money, so none may be lost.
They also report their power draw for a live chart, where a lost sample is replaced by the next one.

## Quality of service

MQTT has three levels of [quality of service](../components/subscriptions/qos.md):

| Level | Guarantee | Packets |
|---|---|---|
| `Qos::AtMostOnce` (0) | at most once: a message may be lost | `PUBLISH` |
| `Qos::AtLeastOnce` (1) | at least once: a message may come twice | `PUBLISH`, `PUBACK` |
| `Qos::ExactlyOnce` (2) | exactly once between two parties | `PUBLISH`, `PUBREC`, `PUBREL`, `PUBCOMP` |

The subscription names the level it reads at, and the publisher names the level it sends at.
A delivery arrives at the lower of the two.
So a meter that publishes its usage at `QoS` 1 reaches a `QoS` 2 subscription at `QoS` 1.

## Settling a report

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust title="src/main.rs" reference="examples/guide/delivery.rs#bill"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust title="src/main.rs" reference="examples/manual/guide/delivery.rs#bill"
```

</TabItem>
</Tabs>

The broker acknowledges a delivery when the handler returns, not when it arrives.
Each [outcome](../components/settlement/index.md) sends what the protocol has for it:

- `ack` sends `PUBACK` at `QoS` 1 and `PUBREC` at `QoS` 2; the client finishes the `QoS` 2 handshake itself.
- `drop` acknowledges too: MQTT has no negative acknowledgement, and acknowledging is its only way to be done with a message.
- [`retry`](../components/settlement/retry.md) sends nothing. The report stays unacknowledged, and the server sends it again only when the service's session resumes on a new connection.

A `QoS` 0 delivery has no acknowledgement at all.
Its outcome reports `AckError::Unsupported`, and the runtime logs a warning.

`bill` returns `retry` when the ledger does not answer.
The report then waits for the next session: after a restart, the server hands it over again.
[Retries and dead letters](retries.md) shows how to try again within the same session.

## Batches at QoS 0

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust title="src/main.rs" reference="examples/guide/delivery.rs#chart"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust title="src/main.rs" reference="examples/manual/guide/delivery.rs#chart"
```

</TabItem>
</Tabs>

A handler over a slice is a [batch handler](../components/subscriptions/batches.md).
A `PUBLISH` packet carries one message, so the broker assembles the batches on the client.
A batch closes when it holds the size the mount site names, or 20 milliseconds after its first sample.
Acknowledgement stays per delivery.

## A persistent session

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust title="src/main.rs" reference="examples/guide/delivery.rs#app"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust title="src/main.rs" reference="examples/manual/guide/delivery.rs#app"
```

</TabItem>
</Tabs>

A [session](../components/broker/session.md) is what the server keeps for one client id: its subscriptions and the `QoS` 1 and 2 messages it has not acknowledged.
[`clean_start(false)`](api:ruststream_rumqttc::MqttBroker::clean_start) resumes the session the server kept, instead of starting a new one.
[`session_expiry`](api:ruststream_rumqttc::MqttBroker::session_expiry) is how long the server keeps it after the service disconnects, a day here.
While the service is down, the server queues the `QoS` 1 and 2 messages that match its subscriptions.
When the service connects again under the same client id, the server sends them first.
The broker holds them until the subscription that owns them opens, so none is lost at startup.
The client id is the session's name, so each process of the service needs its own; [Connecting to production](production.md) shows one per deployment.

Mosquitto does not queue `QoS` 0 messages for an absent session by default.
The samples published while the service is down are gone, which is what a live chart wants.

## Flow control

[`receive_maximum(100)`](../components/broker/limits.md) tells the server how many `QoS` 1 and 2 deliveries the service takes before it acknowledges one.
At the limit the server stops sending until a handler settles a delivery.
The default is 1000.
An unacknowledged delivery holds its place in that window until the connection ends, so every `retry` without a declared cap takes one place away until the next session.
`QoS` 0 deliveries have no such window: a handler slower than its publishers lets them queue in memory without a bound.

## Testing the guarantees

The in-process tests check what `bill` settles for each report and that three samples published at once arrive as one batch.
A session, its resumption and the receive-maximum window live on a server, so one test runs against Mosquitto:

```bash
MQTT_TEST_URL=mqtt://localhost:1883 cargo test -- --test-threads=1
```

It starts the service, ends its connection abruptly, publishes a usage report while the service is down, and starts the service again under the same client id.
The report published while the service was down is the first one `bill` receives.
Without `MQTT_TEST_URL` the test returns at once.
[Testing an MQTT service](testing.md) shows the pattern.
