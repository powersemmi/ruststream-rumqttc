---
sidebar_position: 7
title: Lanes
description: "workers(n, by_key) keeps deliveries that share the partition-key header on one lane, in order, while other keys run in parallel."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Lanes

`workers(n, by_key)` keeps deliveries that share the `partition-key` header on one lane, in order, while other keys run in parallel.

`workers(n, by_key)` runs `n` sequential lanes over one subscription.
The lane of a delivery is picked by a hash of its partition key, so deliveries with one key run one after another, in the order they arrived.
Deliveries with different keys run at once on different lanes.
A delivery without a key goes to the next lane in rotation and keeps no order with any other.

MQTT has no partitions, so the key is the header [`PARTITION_KEY_HEADER`](api:ruststream_rumqttc::PARTITION_KEY_HEADER), `partition-key`, which travels as an MQTT 5 user property.
The protocol gives the property no meaning: it is a convention between the publisher, which sets it, and this crate, which reads it back.
A service that publishes to a keyed subscription sets the header on each message, such as the tenant or the device the message belongs to.
The lanes order work inside one connection.
A [share group](./shared.md) spreads messages over instances and keeps no order by key between them.
The manual form writes the lanes as `.workers_by_key(n)` on the mount chain.

## Example

The checkout keys each order by its tenant.
Orders of one tenant run in order on one lane, and different tenants run side by side.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/mqtt_subscriptions_lanes.rs#lanes"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/mqtt_subscriptions_lanes.rs#lanes"
```

</TabItem>
</Tabs>
