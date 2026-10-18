---
sidebar_position: 3
title: last_will
description: "last_will names the message the server publishes when the connection ends without a clean disconnect, such as a service's offline status."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# last_will

[`last_will`](api:ruststream_rumqttc::MqttBroker::last_will) names the message the server publishes when the connection ends without a clean disconnect, such as a service's offline status.

The will travels in the `CONNECT` packet, and the server keeps it for as long as the connection lasts.
It has a topic, a payload, a QoS and a retain flag, like any publish.
The payload is bytes, written as the service's readers decode them; no codec runs on it.

The server publishes the will when the connection ends any other way than by the client's `DISCONNECT`.
That covers a process that dies, a network that drops, a connection silent past its [keep-alive](./reconnect.md), and a second client that takes the client id over.
A clean [shutdown](./shutdown.md) sends a `DISCONNECT`, and the server discards the will.
A retained will stays on its topic, so a dashboard that subscribes later still reads that the service is gone.

The server publishes the will, so a [`TestApp`](../testing/index.md) test in process runs the app without it.
A [live test](../testing/live.md) shows it.
The generated [AsyncAPI document](../asyncapi/server.md) reports the will's topic, QoS and retain flag, not its payload.

## Example

Each instance announces its own death on `services/<client id>/status`, retained.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/mqtt_broker_last_will.rs#broker"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/mqtt_broker_last_will.rs#broker"
```

</TabItem>
</Tabs>
