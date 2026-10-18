---
sidebar_position: 2
title: Operations
description: "A receive operation reports the QoS its subscription reads at; a send operation reports the QoS and retain flag of its policy."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Operations

A receive operation reports the QoS its subscription reads at; a send operation reports the QoS and retain flag of its policy.

The operation binding carries `qos`, the level as a number: 0, 1 or 2.
A receive operation takes it from the descriptor's [`qos`](../subscriptions/qos.md).
A share group is part of the subscribed filter and adds nothing to the binding.

A send operation comes from an `Out` slot or a [dead-letter topic](../settlement/dead-letters.md).
It reports both arguments of the PUBLISH packet its [publish policy](../publishing/index.md) declares: `qos` and `retain`.
A reply has no send operation of its own, so a reply policy adds nothing to the document's operations.

## Example

The operation `receive_devices___telemetry` reports `{ "qos": 2 }`.
The slot's operation `send_devices___telemetry_alerts_overheat` reports `{ "qos": 1, "retain": true }`, from the policy `Publish::default().retain(true)` the [app](./index.md) attaches.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/mqtt_asyncapi.rs#operations"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/mqtt_asyncapi.rs#operations"
```

</TabItem>
</Tabs>
