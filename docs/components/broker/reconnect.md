---
sidebar_position: 6
title: Reconnect
description: "The connection task reconnects with a backoff from 100 ms to 5 s and resubscribes; keep_alive sets how soon a dead connection is noticed."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Reconnect

The connection task reconnects with a backoff from 100 ms to 5 s and resubscribes; [`keep_alive`](api:ruststream_rumqttc::MqttBroker::keep_alive) sets how soon a dead connection is noticed.

A connection that fails after startup is dialed again after 100 ms.
Each failed attempt doubles the wait, up to 5 seconds, and a successful connect resets it.
A service notices nothing but the gap: its handlers keep running, and its subscriptions stay open.
When the server reports that it no longer has the session, the task subscribes every filter again.
With a persistent [session](./session.md) the server still has it, together with what arrived during the gap.

A refusal the connection will not outgrow ends the task instead: bad credentials, an unacceptable client id, a TLS handshake the certificates cannot satisfy.
Every subscription then returns that error, as [`MqttError::Receive`](api:ruststream_rumqttc::MqttError::Receive).

`keep_alive` is how long the connection may stay silent.
When nothing else is sent for that long, the client pings the server.
The server closes a connection it has heard nothing from for one and a half keep-alive intervals, and publishes its [last will](./last-will.md).
The client notices a server that stopped answering the same way, and reconnects.
The default is 60 seconds, and the client's minimum is 5: a shorter one fails [startup](./startup.md), in a [`TestApp`](../testing/index.md) test as on a server.

## Example

A silent connection is noticed within about 15 seconds.
The session outlives the gap by an hour, so what arrives while the service reconnects is kept for it.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/mqtt_broker_reconnect.rs#broker"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/mqtt_broker_reconnect.rs#broker"
```

</TabItem>
</Tabs>
