---
title: Lifecycle
sidebar_position: 3
description: "MqttBroker::new records configuration, connect waits for the server's CONNACK, startup subscribes each filter, and shutdown sends a DISCONNECT."
---

import {Diagram} from '@site/src/components/Diagrams';

export const words = {
  "title": "The broker's three states as types, and what each transition does on MQTT",
  "text": "The diagram as text",
  "states": "Broker",
  "mqtt": "On MQTT",
  "unconnected": ["MqttBroker::new", "(url, client_id)"],
  "connected": ["ConnectedMqttBroker"],
  "closed": ["Closed: ()", "nothing to report"],
  "config": ["URL, client id", "and settings, no I/O"],
  "options": ["Check the settings:", "port, keep-alive, TLS"],
  "dial": ["Start the task, wait", "30 s for a CONNACK"],
  "subscribe": ["Each subscription:", "SUBSCRIBE, SUBACK"],
  "running": ["Recover a lost connection", "as the reconnect mode says"],
  "disconnect": ["Mark closed, send", "DISCONNECT, flush"],
  "afterShutdown": ["A publisher that outlives the connection", "answers MqttError::NotConnected, never a silent success"],
  "connect": "connect(self)",
  "shutdown": "shutdown(self)"
};

# Lifecycle

[`MqttBroker::new`](../components/broker/index.md) records configuration, connect waits for the
server's CONNACK, startup subscribes each filter, and shutdown sends a DISCONNECT.

<Diagram name="lifecycle" words={words} />

`new` does no I/O, so the service's builder stays synchronous. It takes the URL, `mqtt://host:port`
or `mqtts://host:port`, and the client id, and the builder's settings are recorded beside them.
The runtime connects the broker when the app starts.

Connect checks the settings first, before any I/O. A port that is not a number, a keep-alive under
the client's 5-second minimum and TLS without a certificate authority to trust each fail the start
with [`MqttError::Invalid`](../components/broker/errors.md), naming the setting.

Then connect starts the connection task and waits up to 30 seconds for the server's first
[CONNACK](../components/broker/startup.md). The task retries what the moment may cure, such as a
server not up yet. A refusal the connection cannot outgrow ends the wait at once with
`MqttError::Connect`: bad credentials, a client id the server rejects, a peer that does not speak
MQTT, a TLS handshake the certificates cannot satisfy. A wait that runs out reports the last
connection error it saw.

Each subscription's [descriptor](../components/subscriptions/index.md) resolves once, against the
connected broker. It sends a SUBSCRIBE and waits for the SUBACK, so the server holds the
subscription before the app reports that it started. A filter the server refuses fails the start
with `MqttError::Subscribe`.

While the service runs, the task recovers a lost connection as the broker's
[reconnect mode](../components/broker/reconnect.md) says. By default it waits 100 milliseconds
before the first attempt and doubles the wait up to 5 seconds. When the CONNACK reports the session
gone, it subscribes every filter again. A refusal the connection cannot outgrow ends the task
instead, and so does a mode that gives up: every subscription returns the reason as
`MqttError::Receive`, and its stream ends.

[Shutdown](../components/broker/shutdown.md) marks the connection closed first, so a publisher
that aliases it cannot slip a message in. It then queues a DISCONNECT behind every acknowledgement
already queued, and waits up to 5 seconds for the task to write them all. The server sees a clean
end of the connection and publishes no last will. Shutdown returns nothing more: the closed state
carries no diagnostics.

A publisher paired before the shutdown is a handle the shutdown cannot take back. It answers
`MqttError::NotConnected`. A delivery acknowledged after the shutdown reports a broker error.

The in-process transport follows the same ladder. Its connect checks the settings as `connect`
does, so a configuration the server path refuses is refused in a test too.
