---
sidebar_position: 2
description: "Design MQTT topics, read many with + and # wildcards, share the work between replicas in a share group and mount overlapping filters in one service."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Topics, wildcards and shared subscriptions

This guide reads MQTT topics one at a time and many at once, shares the work between replicas of a service and mounts filters that overlap.

A topic is a name of levels separated by `/`, such as `devices/dev42/telemetry`.
The server hands a message to every subscription whose filter matches its topic at that moment.
It keeps nothing for later, except one retained message per topic ([Device state](device-state.md)) and the queue of a persistent session ([Delivery guarantees](delivery.md)).

The example is a fleet service.
It stores the telemetry of every device, traces every message of one device and schedules firmware rollouts.

## A share group

`store` reads the telemetry of every device.
The service runs in several replicas, and each reading must be stored once.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust title="src/main.rs" reference="examples/guide/topics.rs#store"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust title="src/main.rs" reference="examples/manual/guide/topics.rs#store"
```

</TabItem>
</Tabs>

[`MqttFilter`](../components/subscriptions/mqtt-filter.md) reads every topic a filter matches.
`+` matches exactly one level, so `devices/+/telemetry` reads the telemetry of each device and nothing deeper.
[`shared("ingest")`](../components/subscriptions/shared.md) subscribes `$share/ingest/devices/+/telemetry`, and the server hands each message to one session of the group.
Each replica connects with its own client id and joins the same group, so the replicas split the readings between them.
A subscription outside the group, or in another group, still gets its own copy.

The handler never sees the `$share/..` prefix, and it never sees the filter either.
[`Ctx<DeliveryTopic>`](../components/context/index.md) is the topic this reading arrived on, such as `devices/dev42/telemetry`, so the device's id is in reach.

## Order per device

The server hands each reading of a share group to any member, so the readings of one device may land on different replicas.
Order by device holds inside one subscription: [`workers(n, by_key)`](../components/subscriptions/lanes.md) runs the readings that share a key one after another, and different keys side by side.
The key is the `partition-key` header, [`PARTITION_KEY_HEADER`](api:ruststream_rumqttc::PARTITION_KEY_HEADER), which the device sets on each reading, such as its id.
A subscription whose readings must stay in order per device reads outside a share group.

## A multi-level wildcard

`trace` logs every message of one device, whatever its kind.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust title="src/main.rs" reference="examples/guide/topics.rs#trace"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust title="src/main.rs" reference="examples/manual/guide/topics.rs#trace"
```

</TabItem>
</Tabs>

`#` matches the rest of the topic, any number of levels, and it stands last in a filter.
`devices/dev42/#` reads `devices/dev42/telemetry`, `devices/dev42/telemetry/raw` and every other topic of the device.
The device sends messages of several kinds, so `trace` takes the payload as the bytes it is, and no codec runs on it.

A topic that starts with `$`, such as the server's own `$SYS/..` statistics, is matched only by a filter that starts with `$` too.
So `#` alone never reads them.

## One topic

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust title="src/main.rs" reference="examples/guide/topics.rs#firmware"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust title="src/main.rs" reference="examples/manual/guide/topics.rs#firmware"
```

</TabItem>
</Tabs>

A bare string in `#[subscriber(..)]` is one topic, read with the defaults of [`MqttTopic`](../components/subscriptions/mqtt-topic.md).
`MqttTopic::new("fleet/firmware").qos(..)` writes the same subscription with its settings.
`MqttTopic` takes no wildcard: one in its name fails the start with an error that names `MqttFilter`, before any I/O.
Wildcards belong to subscriptions only, so a publish names one topic, and a publish to a topic with `+` or `#` returns an error.

## The app

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust title="src/main.rs" reference="examples/guide/topics.rs#app"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust title="src/main.rs" reference="examples/manual/guide/topics.rs#app"
```

</TabItem>
</Tabs>

A filter matches many topics, so a delayed copy of its delivery has no single topic to go to.
The mount site names one with [`out_retry(Publish::default()).to(..)`](../components/settlement/out-retry.md), and the service refuses to start without it.
A copy reaches every subscription whose filter matches its topic, so each topic here is matched by its own filter and by no other.
[Retries and dead letters](retries.md) explains the copies.

## Overlapping filters

`store` and `trace` both match `devices/dev42/telemetry`, and they share one connection.
Each of them runs once for a reading on that topic.
The server sends a copy per matching subscription, and the broker tells the copies apart by the [subscription identifier](../components/subscriptions/overlaps.md) each one carries.
Subscription identifiers are optional for a server.
On a server that does not offer them, the service fails to start with an error that names both filters.

The same filter mounted twice in one service is one subscription for the server.
Its handlers take turns on the deliveries instead of each getting a copy.

## Testing the routing

The tests in the example file run the app in process.
They publish to `devices/dev42/telemetry`, `devices/dev7/telemetry` and `devices/dev42/telemetry/raw`, and check which handlers ran.
The in-process server matches topics the way Mosquitto does, wildcards and overlaps included.
Several replicas sharing a group need several connections to one server, so the split itself shows only against a real broker.
