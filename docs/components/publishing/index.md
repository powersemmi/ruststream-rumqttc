---
title: MqttPublish
description: "MqttPublish, Publish in the prelude, sets the [QoS](../subscriptions/qos.md) and the [retain](./retained.md) flag; a publish returns once the client session owns the message."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# MqttPublish

[`MqttPublish`](api:ruststream_rumqttc::MqttPublish), `Publish` in the prelude, sets the QoS and the retain flag; a publish returns once the client session owns the message.

A PUBLISH packet carries two arguments besides the topic: a quality of service and the retain flag.
The policy declares both: [`qos`](api:ruststream_rumqttc::MqttPublish::qos) defaults to [`Qos::AtLeastOnce`](api:ruststream_rumqttc::Qos), and [`retain`](api:ruststream_rumqttc::MqttPublish::retain) to `false`.
A mount site names it wherever a registration publishes: `.out_reply(Publish::default())` for a reply, `.out(Slot, Publish::default())` for a slot, `.out_retry(Publish::default())` for retry copies.
A scope's `after_startup` hook takes one too, for a message the service sends once it is connected.
The policy holds no connection; the runtime pairs it with the connected broker at startup into an [`MqttPublisher`](api:ruststream_rumqttc::MqttPublisher).
A reply mounted without a policy sends through `Publish::default()`, to the topic its type declares.
A publish returns once the client session owns the message, not once the server confirmed it.
At QoS 1 and 2 the session sends the message again until the server acknowledges it, across reconnects.
A topic with a `+` or `#` wildcard is refused with [`MqttError::Publish`](api:ruststream_rumqttc::MqttError::Publish).
One message changes the arguments with the [`qos` and `retain` steps](./mqtt-publish-steps.md).

## Example

A device agent answers each command and each ping on the topic the answer's type names:

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/mqtt_publishing_publish.rs#handlers"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/mqtt_publishing_publish.rs#handlers"
```

</TabItem>
</Tabs>

The acknowledgement goes out exactly once; the pong takes the default policy:

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/mqtt_publishing_publish.rs#app"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/mqtt_publishing_publish.rs#app"
```

</TabItem>
</Tabs>
