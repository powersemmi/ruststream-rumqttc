---
sidebar_position: 2
title: Session
description: "clean_start(false) with a session_expiry keeps the session on the server, so a service that loses its connection gets what arrived meanwhile."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Session

[`clean_start(false)`](api:ruststream_rumqttc::MqttBroker::clean_start) with a [`session_expiry`](api:ruststream_rumqttc::MqttBroker::session_expiry) keeps the session on the server, so a service that loses its connection gets what arrived meanwhile.

An MQTT session belongs to a client id and lives on the server.
It holds the client's subscriptions and the QoS 1 and 2 messages the client has not acknowledged yet.
By default every connect starts a clean session, and the server forgets it when the connection ends.

`clean_start(false)` asks the server to resume the session the client id already has.
`session_expiry` says how long the server keeps it after the connection ends.
The duration counts in whole seconds, and anything longer than `u32::MAX` seconds keeps the session with no end.
Without an expiry the server drops the session with the connection, whatever `clean_start` says.

While the session lives, the server queues every QoS 1 and 2 message that matches its subscriptions.
When the service connects again under the same id, the server sends what it queued, then what arrives next.
This covers a lost connection, a [reconnect](./reconnect.md) and a process that dies.
A clean [shutdown](./shutdown.md) unsubscribes every filter before it disconnects, so after it the session holds no subscription and the server queues nothing for it.

A QoS 0 message is never queued.
A delivery the handler left unacknowledged, such as one [`HandlerOutcome::retry()`](../settlement/retry.md) settled, comes back when the session resumes.

The session lives on the server, so a [`TestApp`](../testing/index.md) test in process starts a fresh one every time.
What a session keeps is for a [live test](../testing/live.md).

## Example

The server keeps the service's session for a day.
A reading published while the service is down reaches it when it comes back.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/mqtt_broker_session.rs#broker"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/mqtt_broker_session.rs#broker"
```

</TabItem>
</Tabs>
