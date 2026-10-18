---
sidebar_position: 7
title: Shutdown
description: "Shutdown sends a clean DISCONNECT and waits up to 5 s for what is queued to leave; a publisher used afterwards reports MqttError::NotConnected."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Shutdown

Shutdown sends a clean `DISCONNECT` and waits up to 5 s for what is queued to leave; a publisher used afterwards reports [`MqttError::NotConnected`](api:ruststream_rumqttc::MqttError::NotConnected).

The runtime stops reading first and lets the handlers in flight finish.
How long they get is the app's [`shutdown_timeout`](api:ruststream::runtime::RustStream::shutdown_timeout), with no bound by default.
Their publishes and acknowledgements join the client's queue as they settle.
Each subscription unsubscribes its filter as it closes, so a persistent [session](./session.md) keeps no subscription past a clean shutdown.

Then the broker sends a `DISCONNECT` behind everything already queued.
It waits up to 5 seconds for the connection task to write the queue out, and logs a warning when they run out.
The server discards the [last will](./last-will.md) of a connection that ends this way.

A publisher taken before the shutdown keeps pointing at the closed connection.
A publish through it returns `MqttError::NotConnected` and never succeeds against the dead connection.
An acknowledgement after the shutdown is refused too.

## Example

The handlers get ten seconds to finish, and their alerts leave before the broker disconnects.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/mqtt_broker_shutdown.rs#app"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/mqtt_broker_shutdown.rs#app"
```

</TabItem>
</Tabs>
