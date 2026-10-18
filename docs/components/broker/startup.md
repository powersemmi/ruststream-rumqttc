---
sidebar_position: 5
title: Startup
description: "Startup waits up to 30 seconds for the server's CONNACK; a refusal the connection cannot outgrow fails it at once, naming the cause."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Startup

Startup waits up to 30 seconds for the server's `CONNACK`; a refusal the connection cannot outgrow fails it at once, naming the cause.

The runtime connects the broker first.
The settings are checked before anything is dialed: a keep-alive under five seconds, an `mqtts://` URL without `tls_ca` or a port that is not a number fail startup with [`MqttError::Invalid`](api:ruststream_rumqttc::MqttError::Invalid).
Then the connection task dials the server.
A server that is down or busy is dialed again with a backoff, until it answers or the 30 seconds run out.
A timeout fails startup with [`MqttError::Connect`](api:ruststream_rumqttc::MqttError::Connect), and the error carries the last reason a dial failed.

Some refusals give the same answer on every attempt: wrong credentials, a client id the server rejects, a peer that is not an MQTT server, and a TLS handshake the certificates cannot satisfy.
Each fails startup at once with `MqttError::Connect`, naming the refusal.
A server that refuses a client certificate after the handshake answers the way a broken connection does, so that refusal is retried and reported when the 30 seconds run out.

Once connected, the runtime opens every subscription, each one waiting for the server's `SUBACK`.
A filter the server rejects fails startup with [`MqttError::Subscribe`](api:ruststream_rumqttc::MqttError::Subscribe).
The `after_startup` hooks run last, so a hook publishes through a connection whose subscriptions are already open.

A [`TestApp`](../testing/index.md) test in process checks the settings as the connect does, then opens the subscriptions and runs the hooks.
The dial and the server's refusals are for a [live test](../testing/live.md).

## Example

Once its subscriptions are open, the service announces that it is online on its status topic.
The announcement is retained, so a dashboard that subscribes later reads it.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/mqtt_broker_startup.rs#app"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/mqtt_broker_startup.rs#app"
```

</TabItem>
</Tabs>
