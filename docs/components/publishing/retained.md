---
sidebar_position: 2
title: Retained
description: "A retained message stays on its topic for every later subscriber of a matching filter; an empty retained payload clears it."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Retained

A retained message stays on its topic for every later subscriber of a matching filter; an empty retained payload clears it.

The server keeps the last retained message of each topic.
A subscription that opens later receives it at once, so a dashboard that starts after a device announced its state still reads that state.
A publisher retains every message with [`Publish::default().retain(true)`](api:ruststream_rumqttc::MqttPublish::retain), and one message with the [`retain` step](./mqtt-publish-steps.md).
Publishing an empty payload retained removes the message the server kept for that topic.
A shared subscription receives no retained messages.
A scope's `after_startup` hook runs once the broker is connected, which is where a service announces its own status.
A state is bytes on the wire rather than an encoded model, so its type carries its own bytes with `#[derive(Serialized)]`, and no codec runs on them.

## Example

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/mqtt_retained.rs#state"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/mqtt_retained.rs#state"
```

</TabItem>
</Tabs>

The service announces its status retained at startup, and each device state through its slot:

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
