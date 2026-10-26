---
title: MqttBroker
description: "MqttBroker connects a service to an MQTT 5 server; new only records the URL and the client id, and the runtime connects it at startup."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# MqttBroker

[`MqttBroker`](api:ruststream_rumqttc::MqttBroker) connects a service to an MQTT 5 server; `new` only records the URL and the client id, and the runtime connects it at startup.

[`MqttBroker::new`](api:ruststream_rumqttc::MqttBroker::new) takes the server's URL, `mqtt://host:port` or `mqtts://host:port`, and the client id.
A URL without a port takes 1883, or 8883 for `mqtts://`.
It does no I/O, so the whole service fits the synchronous `#[ruststream::app]` builder.
The settings that follow it are builder steps too: [credentials and TLS](./security.md), the [session](./session.md), the [last will](./last-will.md), the [limits](./limits.md), the [reconnect mode and the keep-alive](./reconnect.md).
Each page gives the default a setting has when the service leaves it unset.

The client id is the session's identity on the server.
A second connection under the same id takes the session over, and the server closes the first one.
So two instances of one service connect under two ids, and share their work through a [share group](../subscriptions/shared.md).

At [startup](./startup.md) the runtime connects the broker and receives a [`ConnectedMqttBroker`](api:ruststream_rumqttc::ConnectedMqttBroker).
The crate runs one task per connection: it drives the client's event loop, hands each delivery to its subscription, and [reconnects](./reconnect.md) when the connection is lost.
Every subscription and every publisher of the service shares that connection.
At [shutdown](./shutdown.md) the runtime closes it with a clean `DISCONNECT`.

`#[subscriber("devices/dev42/telemetry")]` reads one topic at QoS 1.
A [`MqttFilter`](../subscriptions/mqtt-filter.md) reads many topics through the `+` and `#` wildcards, and the other descriptors are on the [Subscriptions](../subscriptions/index.md) pages.
The crate speaks MQTT 5 only: headers ride its user properties, and share groups exist only there.

The crate has two features, both off by default.
`testing` adds the in-process mode a [`TestApp`](../testing/index.md) test connects, and `asyncapi` adds the [MQTT bindings](../asyncapi/index.md) of the generated document.
The codec is the service's choice: `json`, `msgpack` or `cbor` on `ruststream` itself.

## Example

The service reads the telemetry of every device in the share group `workers`.
A filter names no topic a retry copy could go to, so the mount site names one.
`cargo run --example mqtt_service --features testing -- run` starts it against a server on `localhost:1883`.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/mqtt_service.rs#service"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/mqtt_service.rs#service"
```

</TabItem>
</Tabs>
