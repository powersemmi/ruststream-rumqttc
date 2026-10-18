---
sidebar_position: 3
title: Qos
description: "Qos picks at most once, at least once or exactly once; a delivery arrives at the lower of the publisher's and the subscription's level."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Qos

[`Qos`](api:ruststream_rumqttc::Qos) picks at most once, at least once or exactly once; a delivery arrives at the lower of the publisher's and the subscription's level.

A subscription names its level with `qos(..)` on [`MqttTopic`](./mqtt-topic.md) or [`MqttFilter`](./mqtt-filter.md).
The default is [`Qos::AtLeastOnce`](api:ruststream_rumqttc::Qos::AtLeastOnce).

| Level | QoS | What the protocol does |
| --- | --- | --- |
| `AtMostOnce` | 0 | sends the message once, with no acknowledgement; a message lost on the way is gone |
| `AtLeastOnce` | 1 | resends until the receiver answers `PUBACK`; a message may arrive twice |
| `ExactlyOnce` | 2 | a four-packet handshake, so the message arrives once |

The server sends each message at the lower of two levels: the one its publisher sent it at and the one the subscription asked for.
So a QoS 2 subscription receives a QoS 0 publish at QoS 0.
A delivery at QoS 1 or 2 is [acknowledged](../settlement/index.md) when the handler settles it.
A delivery at QoS 0 has no acknowledgement packet, so settling it reports `AckError::Unsupported`.
The level a publish is sent at is a setting of the [publish policy](../publishing/index.md).

## Example

Telemetry read at most once, because the next reading replaces a lost one, and valve commands read exactly once.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/mqtt_subscriptions_qos.rs#descriptor"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/mqtt_subscriptions_qos.rs#descriptor"
```

</TabItem>
</Tabs>
