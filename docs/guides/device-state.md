---
sidebar_position: 4
description: "Keep each device's current state on the MQTT server with retained messages, clear it, and announce whether the service is online with a last will."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Device state: retained messages and the last will

This guide keeps the current state of every device on the server, so a reader that starts later sees it at once, and announces whether the service itself is running.

MQTT keeps no history, with one exception: a [retained message](../components/publishing/retained.md).
The server keeps the last retained message of each topic and sends it to every new subscription whose filter matches.
The [last will](../components/broker/last-will.md) is a message the server publishes for a client whose connection ends without a clean disconnect.

The example is a gateway monitor.
Gateways send heartbeats with their battery level.
The service keeps the state of each gateway, `ok` or `low-battery`, retained on `gateways/<id>/state`, and dashboards read it from there.

## The state of each gateway

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust title="src/main.rs" reference="examples/guide/device_state.rs#state"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust title="src/main.rs" reference="examples/manual/guide/device_state.rs#state"
```

</TabItem>
</Tabs>

A state is plain text a dashboard shows as it is, so `GatewayState` derives `Serialized` and carries its own bytes, and no codec runs on them.
Its topic is a name template: `{gateway}` becomes a setter, so `.to().gateway(..)` addresses one gateway's topic.
`track` publishes the state of the gateway each heartbeat comes from.

`forget` publishes an empty payload to the state topic of a gateway that is taken out of service.
An empty retained message deletes the retained message of its topic, so the server stops handing that state to new subscribers.

## Retained publishes

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust title="src/main.rs" reference="examples/guide/device_state.rs#app"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust title="src/main.rs" reference="examples/manual/guide/device_state.rs#app"
```

</TabItem>
</Tabs>

[`retain(true)`](api:ruststream_rumqttc::MqttPublish::retain) on the [publish policy](../components/publishing/index.md) makes every publish through the slot retained.
Both handlers share the slot `States` and the policy, so a state and its deletion travel the same way.
One message can also be retained on its own: the [`retain` step](../components/publishing/mqtt-publish-steps.md) on a publish overrides the policy for that call.

A shared subscription does not receive retained messages when it subscribes.
A dashboard that reads the states subscribes outside any share group.

## Online and offline

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust title="src/main.rs" reference="examples/guide/device_state.rs#status"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust title="src/main.rs" reference="examples/manual/guide/device_state.rs#status"
```

</TabItem>
</Tabs>

[`last_will`](api:ruststream_rumqttc::MqttBroker::last_will) takes the topic, the payload, the quality of service and the retain flag of the will.
The broker hands the will to the server when it connects.
If the connection ends without a `DISCONNECT` packet, because the process crashed or the network failed, the server publishes the will: here `offline`, retained on `services/gateway-monitor/status`.
A clean shutdown sends `DISCONNECT`, and the server discards the will.

The other half is the `after_startup` hook in the app above.
It runs once the broker is connected and publishes `online`, retained, to the same topic.
So the status topic always holds the latest of the two, and a dashboard that subscribes reads it at once.

## Testing the state

The in-process tests check what the service publishes: the state of each gateway, the empty payload of a removed one, and `online` at startup.
The in-process server keeps retained messages as Mosquitto does.
The will is published by a server for a connection that ends abruptly, so one test runs against Mosquitto:

```bash
MQTT_TEST_URL=mqtt://localhost:1883 cargo test -- --test-threads=1
```

It runs the service, stops it cleanly, and starts a dashboard afterwards: the dashboard receives `online` and the gateway's state, both retained.
Then it ends a run of the service without a `DISCONNECT`, and the dashboard receives `offline`.
At the end it removes the test's gateway, which clears its state on the server.
