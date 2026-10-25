---
sidebar_position: 6
title: Reconnect
description: "reconnect chooses what a lost connection does: the client dials again from 100 ms up to 5 s by default, never ends every stream, custom runs the service's own reconnection; keep_alive sets how soon a dead connection is noticed."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Reconnect

[`reconnect`](api:ruststream_rumqttc::MqttBroker::reconnect) chooses what a connection lost after startup does: the client dials again by default, and a service may choose no reconnection or one of its own.

A connection ends when the server closes it, sends a `DISCONNECT` (a restart, a keep-alive that ran out), or stops answering.
Three modes handle that, each a [`Reconnect`](api:ruststream_rumqttc::Reconnect):

- [`Reconnect::automatic`](api:ruststream_rumqttc::Reconnect::automatic), the default, dials again after a pause.
  The first pause is 100 ms, and each failed dial doubles it up to 5 seconds.
  [`AutoReconnect`](api:ruststream_rumqttc::AutoReconnect) sets both pauses and a limit of dials; there is no limit by default.
  When the server reports that it no longer has the session, the task subscribes every filter again.
- [`Reconnect::never`](api:ruststream_rumqttc::Reconnect::never) ends every subscription's stream at the first loss, and every publish fails from then on.
- [`Reconnect::custom`](api:ruststream_rumqttc::Reconnect::custom) hands the recovery to a [`Reconnector`](api:ruststream_rumqttc::Reconnector) of the service's own.
  It gets why the connection ended, a dial that installs the new connection for every publisher, the open subscriptions and the helpers that subscribe them again.
  It decides how to dial, what to restore and when to give up.

While a mode recovers, the handlers keep running and the subscriptions stay open.
With a persistent [session](./session.md) the server keeps the subscriptions and what arrived during the gap.

A refusal that retrying cannot change ends the task in every mode: bad credentials, a withdrawn authorization, an unacceptable client id, a TLS handshake the certificates cannot satisfy.
A mode that gives up ends the task too.
Every subscription then returns the reason as [`MqttError::Receive`](api:ruststream_rumqttc::MqttError::Receive), and its stream ends.
The application's [`on_subscription_end`](api:ruststream::runtime::RustStream::on_subscription_end) setting decides whether the service stops.

`keep_alive` is how long the connection may stay silent.
When nothing else is sent for that long, the client pings the server.
The server closes a connection it has heard nothing from for one and a half keep-alive intervals, and publishes its [last will](./last-will.md).
The client notices a server that stopped answering the same way, and reconnects.
The default is 60 seconds, and the client's minimum is 5: a shorter one fails [startup](./startup.md), in a [`TestApp`](../testing/index.md) test as on a server.

## Example

A silent connection is noticed within about 15 seconds.
The session outlives the gap by an hour, so what arrives while the service reconnects is kept for it.
After 120 failed dials, about ten minutes, every subscription ends, and `SubscriptionEnd::FailFast` stops the service with an error.

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

## A reconnection of the service's own

This one logs in with a fresh password on every dial and gives up after a minute.
A session the server did not keep gets every subscription again, in the order they were opened.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/mqtt_broker_reconnect.rs#custom"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/mqtt_broker_reconnect.rs#custom"
```

</TabItem>
</Tabs>
