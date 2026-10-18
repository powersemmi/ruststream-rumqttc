---
sidebar_position: 5
title: out_retry
description: "A filter subscription names where its retry copies go, once with out_retry(..).to(..) or per delivery with a transform."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# out_retry

A filter subscription names where its retry copies go, once with [`out_retry(..).to(..)`](api:ruststream::runtime::Mounting::out_retry) or per delivery with a transform.

An [`MqttTopic`](../subscriptions/mqtt-topic.md) reads one topic, and its copies go there; a share group's copies go to the plain topic, not to `$share/..`.
An [`MqttFilter`](../subscriptions/mqtt-filter.md) matches many topics and is a topic of its own only by accident, so the mount site names the destination.
`out_retry` binds the [`Retry`](api:ruststream::runtime::Retry) slot, the one the runtime publishes its copies through, to a publish policy.
`.to(topic)` sends every copy to one topic; a topic the filter matches brings the copies back to the same handler.
A publish transform that declares [`Names`](api:ruststream::runtime::Names) picks the topic per delivery instead, such as the topic the delivery arrived on.
It reads that topic from the [`MqttContext`](../context/index.md) of the delivery, which reaches the slot when the handler reads it: a `Ctx<DeliveryTopic>` parameter, or a manual body typed over `MqttContext`.
Naming a topic and adding a naming transform are mutually exclusive, and writing both does not compile.
A filter registration that names neither does not start, and the error names the subscription.
The copy carries the delivery's own bytes, so a codec on the slot encodes nothing, while a transform still runs.

## Example

A shared fleet subscription sends its copies to one retry topic its filter matches:

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/mqtt_retries.rs#named"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/mqtt_retries.rs#named"
```

</TabItem>
</Tabs>

A site subscription sends each copy back to the topic it arrived on:

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/mqtt_retries.rs#naming_transform"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/mqtt_retries.rs#naming_transform"
```

</TabItem>
</Tabs>

The mount site names the destination of each filter subscription:

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/mqtt_retries.rs#declaration"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/mqtt_retries.rs#declaration"
```

</TabItem>
</Tabs>
