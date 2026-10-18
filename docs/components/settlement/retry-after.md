---
sidebar_position: 2
title: retry_after
description: "retry_after acknowledges the delivery, waits, then publishes a copy of the message with its retry count in a header."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# retry_after

[`retry_after(delay)`](api:ruststream::runtime::HandlerOutcome::retry_after) acknowledges the delivery, waits, then publishes a copy of the message with its retry count in a header.

MQTT holds no message back and counts no deliveries, so the runtime does both.
It acknowledges the original, waits out the delay and publishes the payload and the headers as they arrived.
The copy carries [`RETRY_COUNT_HEADER`](api:ruststream::runtime::RETRY_COUNT_HEADER), raised by one on every copy, and the handler reads it like any other header.
An [`MqttTopic`](../subscriptions/mqtt-topic.md) subscription receives its copies on its own topic; an [`MqttFilter`](../subscriptions/mqtt-filter.md) names where they go with [`out_retry`](./out-retry.md).
The copy waits in the service's memory, so a process that exits before the delay is over loses it.
A QoS 0 delivery has no acknowledgement, and its copy is published all the same.
A copy is an ordinary publish, so every subscription whose filter matches its topic receives it, in this service and in any other.
A retried topic therefore lies outside every other filter the service reads, or another handler reads the copy too.
[`max_attempts`](./retries.md) caps the deliveries of one message.

## Example

A reading the database timed out on is written again five seconds later.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/mqtt_retries.rs#retry_after"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/mqtt_retries.rs#retry_after"
```

</TabItem>
</Tabs>
