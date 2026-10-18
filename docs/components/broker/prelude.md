---
sidebar_position: 8
title: Prelude
description: "One glob, ruststream_rumqttc::prelude, brings the core prelude, the descriptors, Qos, the policy as Publish and the context key."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Prelude

One glob, [`ruststream_rumqttc::prelude`](api:ruststream_rumqttc::prelude), brings the core prelude, the descriptors, `Qos`, the policy as `Publish` and the context key.

`use ruststream_rumqttc::prelude::*;` is the one import a routes file on MQTT writes.
It carries the core's own prelude, so `#[subscriber]`, `RustStream`, `HandlerOutcome` and `Out` come with it.
It adds [`MqttBroker`](./index.md), the descriptors [`MqttTopic`](../subscriptions/mqtt-topic.md) and [`MqttFilter`](../subscriptions/mqtt-filter.md), their quality of service [`Qos`](../subscriptions/qos.md), and the [context](../context/index.md) type `MqttContext` with its key `DeliveryTopic`.

The publish policy comes under the name every broker crate uses: `Publish` is [`MqttPublish`](../publishing/index.md).
So a mount site reads the same on every broker.
Two broker preludes in one file collide on that name; the prefixed `MqttPublish` at the crate root tells them apart.

A handler body bounds its slots with capability traits, such as `Out<impl Publisher>`, and names no broker type.
So a file of handlers imports `ruststream::prelude::*` alone.
The one exception is a body that sets the QoS or the retain flag of one message: it needs [`MqttPublishSteps`](../publishing/mqtt-publish-steps.md) in scope and the slot bounded `Out<impl Publisher<Options = MqttPublishOptions>, Marker>`, both from this prelude.
[`MqttError`](./errors.md) is named where it is handled, from the crate root.

## Example

The descriptor, its QoS, the context key and the policy all come from the one glob.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/mqtt_broker_prelude.rs#app"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/mqtt_broker_prelude.rs#app"
```

</TabItem>
</Tabs>
