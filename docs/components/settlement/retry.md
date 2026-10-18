---
sidebar_position: 1
title: retry
description: "retry leaves a delivery unacknowledged: it comes back when a persistent session resumes, never within the connection."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# retry

[`retry()`](api:ruststream::runtime::HandlerOutcome::retry) leaves a delivery unacknowledged: it comes back when a persistent session resumes, never within the connection.

The outcome asks the broker for a negative acknowledgement, and MQTT has none.
The runtime logs the refusal and moves on, and nothing acknowledges the delivery later.
At QoS 1 and 2 the server keeps the message in the client's session.
It sends the message again when the service connects with the same client id and the session is still there: [`clean_start(false)`](api:ruststream_rumqttc::MqttBroker::clean_start) and a [`session_expiry`](api:ruststream_rumqttc::MqttBroker::session_expiry) longer than the gap.
With a clean start, or once the session expired, the message is gone; at QoS 0 it is gone at once.
Until the connection ends, the delivery holds one slot of the [`receive_maximum`](api:ruststream_rumqttc::MqttBroker::receive_maximum) window, 1000 by default.
When the window is full of unacknowledged deliveries, the server sends no more QoS 1 or 2 messages on that connection.
For a retry within the session, a handler returns [`retry_after`](./retry-after.md).
A registration with a [cap](./retries.md) turns `retry()` into an immediate copy of the message instead.

## Example

An update that needs a reboot waits for the next session of the device agent.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/mqtt_settlement_retry.rs#handler"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/mqtt_settlement_retry.rs#handler"
```

</TabItem>
</Tabs>

The broker keeps the session for a day after the connection ends:

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/mqtt_settlement_retry.rs#app"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/mqtt_settlement_retry.rs#app"
```

</TabItem>
</Tabs>
