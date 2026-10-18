---
sidebar_position: 1
title: MqttTopic
description: "MqttTopic subscribes to one topic with no wildcard, so a retry copy published to that topic comes back with nothing named at the mount site."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# MqttTopic

[`MqttTopic`](api:ruststream_rumqttc::MqttTopic) subscribes to one topic with no wildcard, so a retry copy published to that topic comes back with nothing named at the mount site.

[`MqttTopic::new`](api:ruststream_rumqttc::MqttTopic::new) takes the topic.
[`qos`](api:ruststream_rumqttc::MqttTopic::qos) sets the [quality of service](./qos.md), `AtLeastOnce` by default.
[`shared`](api:ruststream_rumqttc::MqttTopic::shared) puts the subscription in a [share group](./shared.md).
The subscription's name is the topic: a test reads its deliveries under that name, and the topic is the channel of the [AsyncAPI document](../asyncapi/index.md).
A topic is also a name a publisher can use.
So a [`retry_after`](../settlement/retry-after.md) copy is published to the subscription's own topic, and the mount site names no destination for it.
The copy is an ordinary publish, so every subscription whose filter matches the topic receives it too.
A share group does not change where the copy goes: it is published to the plain topic.
An empty topic, a topic with a `+` or `#` wildcard, or one with a control character stops the service at startup, before anything is subscribed.
A wildcard belongs to [`MqttFilter`](./mqtt-filter.md), and the error names it.

## Example

Commands for one valve, read at QoS 2; a command the valve refuses comes back five seconds later.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/mqtt_subscriptions_topic.rs#descriptor"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/mqtt_subscriptions_topic.rs#descriptor"
```

</TabItem>
</Tabs>
