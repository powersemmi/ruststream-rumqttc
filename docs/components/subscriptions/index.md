---
title: Subscriptions
description: "A topic in #[subscriber] subscribes to that one topic at QoS 1; a descriptor carries wildcards, the quality of service and a share group."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Subscriptions

A topic in `#[subscriber]` subscribes to that one topic at QoS 1; a descriptor carries wildcards, the quality of service and a share group.

`#[subscriber("devices/dev42/telemetry")]` subscribes to the topic `devices/dev42/telemetry`.
The bare name is an [`MqttTopic`](./mqtt-topic.md) with its defaults: [`Qos::AtLeastOnce`](./qos.md) and no share group.
A name with a `+` or `#` wildcard is a filter, not a topic, so the service does not start, and the error names [`MqttFilter`](./mqtt-filter.md), the descriptor that takes it.
Every other setting belongs to a descriptor: `MqttTopic` for one topic, `MqttFilter` for a filter with wildcards.
Each descriptor carries its [quality of service](./qos.md) and its [share group](./shared.md).
A descriptor goes where the name goes, as the attribute's first argument.
The manual form passes the name to [`subscriber(name, body)`](api:ruststream::runtime::subscriber), and a descriptor joins the chain through [`map_source`](api:ruststream::runtime::SubscriberSettings::map_source).
The subscription is open once the server has acknowledged it, before the service takes its first message.

## Example

The readings of the device `dev42`, on one topic.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/mqtt_subscriptions_name.rs#handler"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/mqtt_subscriptions_name.rs#handler"
```

</TabItem>
</Tabs>
