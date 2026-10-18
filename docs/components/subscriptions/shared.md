---
sidebar_position: 4
title: shared
description: "shared(group) makes a share group: the server hands each message to one member, and retained messages reach none of them."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# shared

[`shared(group)`](api:ruststream_rumqttc::MqttFilter::shared) makes a share group: the server hands each message to one member, and retained messages reach none of them.

A share group is how MQTT 5 spreads messages over competing consumers.
`MqttFilter::new("devices/+/telemetry").shared("workers")` subscribes `$share/workers/devices/+/telemetry`.
[`MqttTopic::shared`](api:ruststream_rumqttc::MqttTopic::shared) does the same for one topic.
Every instance of a service that subscribes with the same group and the same filter is a member, and the server sends each message to one member only.
A subscription outside the group on the same filter still receives every message.

The group belongs to the subscribed filter alone.
The subscription's name, the delivery's [topic](../context/index.md) and the destination of a [retry copy](./mqtt-topic.md) stay the plain form.
A [retained message](../publishing/retained.md) is not sent to a share group.
An empty group name, or one with `/`, `+` or `#`, stops the service at startup.
Two members of one group on one connection are one subscription to the server, and the crate hands their deliveries out in turn.

## Example

Each instance of the telemetry service joins `workers`, so the fleet's readings are spread over the instances.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/mqtt_subscriptions_shared.rs#descriptor"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/mqtt_subscriptions_shared.rs#descriptor"
```

</TabItem>
</Tabs>
