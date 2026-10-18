---
sidebar_position: 6
description: "Retry an MQTT message now or later, cap the attempts, move what keeps failing to a dead-letter topic, and send the copies of a filter back to their own topics."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Retries and dead letters

This guide retries a message now or later, caps the attempts and moves what keeps failing to a [dead-letter topic](../components/settlement/dead-letters.md).

MQTT holds no message back, counts no deliveries and has no dead-letter queue.
The framework does all three: the mount site declares the cap and the dead-letter topic, and a retry travels as a copy of the message with a retry count in a header.

The example is a firmware service.
It installs updates on devices, and a device may refuse an update for a while.
It also stores the reports every device sends.

## The subscriber

A device refuses an update in three ways, and each needs a different outcome.
`Busy` means it is running a job: an update offered now is refused again, so [`retry_after`](../components/settlement/retry-after.md) offers it in 30 seconds.
`Interrupted` means the transfer broke off: [`retry`](../components/settlement/retry.md) tries again at once.
`Incompatible` means the image does not fit the device, and another try gets the same answer, so the update is dropped.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust title="src/main.rs" reference="examples/guide/retries.rs#install"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust title="src/main.rs" reference="examples/manual/guide/retries.rs#install"
```

</TabItem>
</Tabs>

`retry_after` acknowledges the delivery, waits, then publishes a copy back to `updates/install`.
The copy carries the retry-count header ([`RETRY_COUNT_HEADER`](api:ruststream::runtime::RETRY_COUNT_HEADER)), raised by one.
It works at every quality of service; a `QoS` 0 delivery has nothing to acknowledge, and its copy is published all the same.
The copy waits in the service's memory: a process that stops before the delay ends loses it.

`retry` becomes a copy too, published at once, because the mount site below declares a cap.
MQTT counts no redeliveries, so the cap counts the copies through their header.
Without a cap, `retry` leaves the delivery unacknowledged for the next session, as [Delivery guarantees](delivery.md) shows.

## The cap and the dead-letter topic

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust title="src/main.rs" reference="examples/guide/retries.rs#app"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust title="src/main.rs" reference="examples/manual/guide/retries.rs#app"
```

</TabItem>
</Tabs>

[`max_attempts`](../components/settlement/retries.md) counts every delivery of one update, the first included.
The fourth delivery is not retried: [`dead_letter`](api:ruststream::runtime::RouterWith::dead_letter) publishes it to `updates/dead`, payload and headers as they arrived.
A dropped update settles and goes nowhere; the dead-letter topic holds only the updates that ran out of attempts.

A copy is an ordinary publish, so every subscription whose filter matches its topic receives it, in this service and in any other.
So each retried topic and each dead-letter topic lies outside every other filter the service reads.
An `updates/#` subscription next to `install` would read every copy of every update, and every dead letter too.

## Reading the dead-letter topic

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust title="src/main.rs" reference="examples/guide/retries.rs#review"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust title="src/main.rs" reference="examples/manual/guide/retries.rs#review"
```

</TabItem>
</Tabs>

The dead-letter topic is an ordinary topic, and `review` reads it like any other.
The retry-count header of a dead-lettered update holds the number of deliveries it went through, 4 here.

## Retries over a filter

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust title="src/main.rs" reference="examples/guide/retries.rs#reports"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust title="src/main.rs" reference="examples/manual/guide/retries.rs#reports"
```

</TabItem>
</Tabs>

A copy of a delivery on an [`MqttTopic`](../components/subscriptions/mqtt-topic.md) goes back to that topic.
An [`MqttFilter`](../components/subscriptions/mqtt-filter.md) matches many topics and names none, so its registration names where a copy goes, and the service refuses to start without it.
[`out_retry(Publish::default()).to(..)`](../components/settlement/out-retry.md) names one topic for every copy.
`ToDeliveryTopic` names one per delivery instead: it reads the topic the report arrived on from the [MQTT context](../components/context/index.md) and sends the copy back there.
So a report from `devices/dev7/reports` comes back on `devices/dev7/reports`, to `record` alone.
The handler takes `Ctx<DeliveryTopic>`, which gives the registration the MQTT context; in the manual form its context type is `MqttContext`.

## Testing the delays

The tests in the example file run the app in process with a paused clock, so `tb.advance(BACKOFF)` lets the 30 seconds pass at once.
They count the deliveries of each update, read the retry-count header of each copy, and check what reaches `updates/dead`.
[Testing an MQTT service](testing.md) covers the harness.
