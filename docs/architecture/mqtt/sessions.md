---
title: Sessions
sidebar_position: 2
description: "An MQTT session holds a client's subscriptions and unacknowledged messages; a persistent one outlives the connection and resumes on reconnect."
---

import {Diagram} from '@site/src/components/Diagrams';

export const words = {
  "title": "A persistent session from the first CONNECT to its resumption after a lost connection",
  "text": "The diagram as text",
  "serviceLane": "This service",
  "serverLane": "MQTT server",
  "connect": ["CONNECT: client id,", "clean start false,", "session expiry, will"],
  "connack": ["CONNACK", "session present: no"],
  "ping": ["PINGREQ when idle", "for the keep-alive"],
  "lost": ["The connection is lost,", "no DISCONNECT"],
  "will": ["The server publishes", "the last will"],
  "kept": ["It keeps the session:", "subscriptions and", "unacknowledged messages"],
  "reconnect": ["CONNECT again:", "the same client id,", "clean start false"],
  "present": ["CONNACK", "session present: yes"],
  "resend": ["The kept messages", "are sent again"]
};

# Sessions

An MQTT session holds a client's subscriptions and unacknowledged messages; a persistent one
outlives the connection and resumes on reconnect.

<Diagram name="sessions" words={words} />

The client id names the session. [`MqttBroker::new`](../../components/broker/index.md) takes it
beside the URL, and a second connection with the same id takes the session over.

Two settings of the CONNECT decide whether the session outlives the connection.
[`clean_start(false)`](../../components/broker/session.md) asks the server to resume the session
the client id already has. `session_expiry` says how long the server keeps it after the connection
ends, in whole seconds. Without them a connection starts a clean session, which ends with the
connection.

The CONNACK answers whether the server found the session. When it reports none, this crate
subscribes every filter again. When the session is present, the subscriptions are still on the
server, and the QoS 1 and QoS 2 messages that arrived while the service was away, or that it never
acknowledged, come next. Those messages can arrive before the subscription they belong to has
opened, so the crate holds up to 1024 of them until it does.

The keep-alive is the longest silence the connection allows. When nothing else travels, the client
sends a PINGREQ, and the server ends a connection that stays silent for one and a half times the
interval. `rumqttc` asks for 60 seconds unless
[`keep_alive`](../../components/broker/reconnect.md) names another, and its minimum is 5 seconds.

The [last will](../../components/broker/last-will.md) is a message the CONNECT leaves with the
server. The server publishes it when the connection ends without a DISCONNECT: a crash, a lost
network, a keep-alive that ran out. A [shutdown](../../components/broker/shutdown.md) sends the
DISCONNECT, so a service that stops cleanly publishes no will.

Two more settings of the CONNECT bound what the server sends. The Receive Maximum is drawn on
[Quality of service](qos.md). The Maximum Packet Size is the largest packet the client accepts,
and the server discards a message larger than that for this client instead of sending it. This
crate announces 1 MiB by default, and [`max_packet_size`](../../components/broker/limits.md)
changes it. The CONNACK also says whether the server offers subscription identifiers, which
[Demultiplexing](../demultiplexing.md) relies on.

The session is the protocol's own, from sections 3.1, 3.2 and 4.1 of the
[MQTT 5.0 standard](https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html). How this
crate opens and closes it is on [Lifecycle](../lifecycle.md).
