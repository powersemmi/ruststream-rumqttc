---
sidebar_position: 2
title: MqttFilter
description: "MqttFilter subscribes with the + and # wildcards; each delivery carries its own topic, and the mount site names where retry copies go."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# MqttFilter

[`MqttFilter`](api:ruststream_rumqttc::MqttFilter) subscribes with the `+` and `#` wildcards; each delivery carries its own topic, and the mount site names where retry copies go.

[`MqttFilter::new`](api:ruststream_rumqttc::MqttFilter::new) takes the topic filter.
`+` matches exactly one level of a topic, and `#` matches every level that remains, so it may only come last.
A filter that starts with a wildcard does not match a topic that starts with `$`, such as the server's own `$SYS` topics.
[`qos`](api:ruststream_rumqttc::MqttFilter::qos) sets the [quality of service](./qos.md), and [`shared`](api:ruststream_rumqttc::MqttFilter::shared) puts the subscription in a [share group](./shared.md).
The subscription's name is the filter.
A handler reads the topic each message was published to through [`DeliveryTopic`](../context/index.md).
A filter with `#` before its last level, or one with a control character, stops the service at [startup](../broker/startup.md).

A filter is no topic a publisher can use, so the subscription has no topic of its own for a [`retry_after`](../settlement/retry-after.md) copy.
The mount site names one with `.out_retry(policy).to(topic)`, or names one per delivery with a [transform](../settlement/out-retry.md).
A registration that names neither does not start, and the error names the subscription and `MqttFilter`.
A destination the filter matches brings the copy back to this subscription.

## Example

The readings of every device, read at QoS 1; a retry copy goes to `devices/retry/telemetry`.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/mqtt_subscriptions_filter.rs#descriptor"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/mqtt_subscriptions_filter.rs#descriptor"
```

</TabItem>
</Tabs>
