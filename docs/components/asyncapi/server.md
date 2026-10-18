---
sidebar_position: 1
title: Server
description: "The server reports host and port with MQTT 5 and the session binding: client id, clean start, keep-alive, expiry, last will, never credentials."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Server

The server reports host and port with MQTT 5 and the session binding: client id, clean start, keep-alive, expiry, last will, never credentials.

A broker registered with `with_broker_labeled` is a server of the document, under its label.
Its `host` is the host and port of the broker's URL.
A URL without a port reports the protocol's default: 1883 for `mqtt://`, 8883 for `mqtts://`.
The `protocol` is `mqtt` and the `protocolVersion` is `5`, the only version the crate speaks.

The server binding describes the session the client opens.
`clientId` is the client id of [`MqttBroker::new`](api:ruststream_rumqttc::MqttBroker::new), and `maximumPacketSize` is the [packet limit](../broker/limits.md), 1 MiB by default.
`cleanSession`, `keepAlive` and `sessionExpiryInterval` appear when the broker sets [`clean_start`](../broker/session.md), [`keep_alive`](../broker/reconnect.md) and [`session_expiry`](../broker/session.md).
`lastWill` reports the [last will](../broker/last-will.md)'s topic, quality of service and retain flag.
The will's payload is the content of a message, so it stays out.
The user name and password of [`credentials`](../broker/security.md) stay out too.

## Example

The broker logs in, keeps its session for an hour and leaves a last will.
Its server in the document is `mqtt.internal:1883`, with the session below and no password.

```json
{
  "bindingVersion": "0.2.0",
  "clientId": "telemetry-svc",
  "cleanSession": false,
  "keepAlive": 30,
  "sessionExpiryInterval": 3600,
  "maximumPacketSize": 1048576,
  "lastWill": { "topic": "devices/svc/status", "qos": 1, "retain": true }
}
```

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/mqtt_asyncapi.rs#server"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/mqtt_asyncapi.rs#server"
```

</TabItem>
</Tabs>
