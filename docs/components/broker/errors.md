---
sidebar_position: 9
title: Errors
description: "MqttError is the one error type of the crate: the connect, a subscription, a delivery, a publish, a closed connection and an invalid setting."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Errors

[`MqttError`](api:ruststream_rumqttc::MqttError) is the one error type of the crate: the connect, a subscription, a delivery, a publish, a closed connection and an invalid setting.

Each variant names the step that failed.
The client's errors become text inside it, so the `rumqttc` types stay out of the crate's API.

| Variant | When |
|---|---|
| `Connect` | the server refused the connection, the TLS handshake failed, or no `CONNACK` came within 30 seconds |
| `Subscribe` | the server rejected a filter, or the connection ended while a subscription opened |
| `Receive` | the connection ended for good: the server refused a reconnect for a reason no retry changes |
| `Publish` | the topic is not one a server takes (empty, with a wildcard or a control character), a header has no MQTT form, or the connection task is gone |
| `NotConnected` | a publisher was used, or a subscription opened, after the broker shut down |
| `Invalid` | a setting or a descriptor cannot work, such as a keep-alive under five seconds or a share group with a `/` |

A service meets the error in two places.
A failed [startup](./startup.md) carries it as the source of the runtime's error, and the service exits before it reads anything.
A publish from a handler returns it inside the core's `PublishError`, and the handler decides how the delivery settles.
The enum is `#[non_exhaustive]`, so a `match` on it ends with a wildcard arm.

## Example

An alert goes to the topic of its device.
A device id with a wildcard makes no topic, so the handler logs the refusal and drops the reading: a retry would fail the same way.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/mqtt_broker_errors.rs#handler"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/mqtt_broker_errors.rs#handler"
```

</TabItem>
</Tabs>
