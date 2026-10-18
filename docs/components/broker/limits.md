---
sidebar_position: 4
title: Limits
description: "max_packet_size caps what the server may send, 1 MiB by default; receive_maximum caps unacknowledged QoS 1 and 2 deliveries, 1000 by default."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Limits

[`max_packet_size`](api:ruststream_rumqttc::MqttBroker::max_packet_size) caps what the server may send, 1 MiB by default; [`receive_maximum`](api:ruststream_rumqttc::MqttBroker::receive_maximum) caps unacknowledged QoS 1 and 2 deliveries, 1000 by default.

Both travel to the server in the `CONNECT` packet, and the server holds itself to them.
Both take a non-zero type, `NonZeroU32` and `NonZeroU16`, because a server refuses a session that announces a zero limit.
A constant such as `const IN_FLIGHT: NonZeroU16 = NonZeroU16::new(100).unwrap();` is checked when the service compiles.
A message larger than `max_packet_size` never reaches the service: the server drops it for this client and delivers it to everyone else.
The `rumqttc` client alone would allow 10 KiB and close the connection on a larger packet, so the crate raises its default to 1 MiB.
A service whose payloads are larger raises it to fit them.

`receive_maximum` is MQTT's flow control.
The server sends QoS 1 and 2 deliveries until that many wait for an acknowledgement, then holds the rest until one is settled.
So it also bounds what queues up in front of a slow handler.
A delivery left unacknowledged holds its place until the connection ends: [`HandlerOutcome::retry()`](../settlement/retry.md), for one, acknowledges nothing.
Once every place is held, the server delivers no QoS 1 or 2 message on the connection, to any subscription, until the connection ends.
QoS 0 deliveries need no acknowledgement and are not bounded.

A [`TestApp`](../testing/index.md) test in process drops a message over `max_packet_size` as the server does.
The window lives on the server, so it is for a [live test](../testing/live.md).

## Example

Firmware images reach 256 KiB, and at most 100 deliveries wait for an acknowledgement.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/mqtt_broker_limits.rs#broker"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/mqtt_broker_limits.rs#broker"
```

</TabItem>
</Tabs>
