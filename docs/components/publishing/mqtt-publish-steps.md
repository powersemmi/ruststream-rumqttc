---
sidebar_position: 1
title: MqttPublishSteps
description: "The qos and retain steps change one message's QoS or retain flag over the policy; a handler bounds its slot with MqttPublishOptions."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# MqttPublishSteps

The [`qos`](api:ruststream_rumqttc::MqttPublishSteps::qos) and [`retain`](api:ruststream_rumqttc::MqttPublishSteps::retain) steps change one message's QoS or retain flag over the policy; a handler bounds its slot with [`MqttPublishOptions`](api:ruststream_rumqttc::MqttPublishOptions).

[`MqttPublishSteps`](api:ruststream_rumqttc::MqttPublishSteps) puts both steps on the publish builder, in any order.
An argument the call does not name keeps what the [policy](./index.md) declared at the mount site.
The values reach the client as the protocol fields they are, so a subscriber receives a plain message with no extra property.
A publish with no call site of its own, a reply or a retry copy, takes the policy whole.
The steps are reached from a slot whose bound names the options type: `Out<impl Publisher<Options = MqttPublishOptions>, Slot>`.
That bound and this crate's prelude are the one place a handler body names the broker.
A [test](../testing/index.md) reads what the steps set with `tb.out::<Slot>().with_options(&MqttPublishOptions::default().retain(true))`.

## Example

A reading changes the state of its device, and the handler publishes that one state [retained](./retained.md):

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/mqtt_retained.rs#stepped_handler"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/mqtt_retained.rs#stepped_handler"
```

</TabItem>
</Tabs>

The mount site fixes the slot's policy, which the step changes for one message:

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/mqtt_retained.rs#app"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/mqtt_retained.rs#app"
```

</TabItem>
</Tabs>
