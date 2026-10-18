---
title: MqttContext
description: "Ctx<DeliveryTopic> gives a handler the topic a message was published to, which differs from the filter that matched it."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# MqttContext

`Ctx<DeliveryTopic>` gives a handler the topic a message was published to, which differs from the filter that matched it.

[`MqttContext`](api:ruststream_rumqttc::MqttContext) is what a delivery carries besides its payload and headers.
The subscription builds it once per delivery.
It holds one field, the topic, under the key [`DeliveryTopic`](api:ruststream_rumqttc::DeliveryTopic).
Both are in the prelude.
A handler binds the topic as a parameter with [`Ctx<DeliveryTopic>`](api:ruststream::runtime::Ctx), which hands it over as a `String`.
A body that takes `ctx: &mut Context<'_, MqttContext>` reads it borrowed with `ctx.context(DeliveryTopic)`, as the manual form does.

On an [`MqttFilter`](../subscriptions/mqtt-filter.md) the topic tells the messages of the filter apart: `devices/dev42/telemetry` and `devices/dev43/telemetry` both match `devices/+/telemetry`.
The subscription's name, `ctx.name()`, is the filter, never the topic.
The same key reads the topic in a publish transform on the reply or the retry position, through `cx.context(DeliveryTopic)`; a [transform on the retry position](../settlement/out-retry.md) sends a copy back to the topic its delivery arrived on.

## Example

An alarm names no device; the topic it arrived on does.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/mqtt_context.rs#handler"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/mqtt_context.rs#handler"
```

</TabItem>
</Tabs>
