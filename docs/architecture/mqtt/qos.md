---
title: Quality of service
sidebar_position: 1
description: "MQTT acknowledges a delivery with one packet at QoS 1 and a four-packet handshake at QoS 2; this crate sends the acknowledgement when the handler is done."
---

import {Diagram} from '@site/src/components/Diagrams';

export const words = {
  "title": "One delivery at each quality of service: the packets the server and the service exchange, and where the handler runs",
  "text": "The diagram as text",
  "serverLane": "MQTT server",
  "serviceLane": "This service",
  "qos0": "QoS 0: at most once",
  "qos1": "QoS 1: at least once",
  "qos2": "QoS 2: exactly once",
  "publish0": "",
  "publish1": "",
  "publish2": "",
  "handler0": ["The handler runs"],
  "handler1": ["The handler runs"],
  "handler2": ["The handler runs"],
  "puback": "on ack",
  "pubrec": "on ack",
  "pubrel": "",
  "pubcomp": "by rumqttc"
};

# Quality of service

MQTT acknowledges a delivery with one packet at QoS 1 and a four-packet handshake at QoS 2; this
crate sends the acknowledgement when the handler is done.

<Diagram name="qos" words={words} />

The quality of service is chosen twice: by the publisher for its message, and by the subscription
for what it accepts. A delivery travels at the lesser of the two, so a QoS 0 publish reaches a
QoS 1 subscription at QoS 0. The [`Qos`](../../components/subscriptions/qos.md) of a descriptor
sets the subscription's side, and the [publish policy](../../components/publishing/index.md) sets
the message's.

At QoS 0 the server sends the PUBLISH and forgets it. Nothing travels back, so a QoS 0 delivery has
nothing to acknowledge: `ack` reports `AckError::Unsupported`, and the runtime logs it.

At QoS 1 and QoS 2 the server keeps the message until the service answers. This crate connects with
manual acknowledgements, so `rumqttc` hands each PUBLISH to the subscription and sends nothing on
its own. The [acknowledgement](../../components/settlement/index.md) leaves when the delivery is
acknowledged, after the handler returns: PUBACK at QoS 1, PUBREC at QoS 2. The PUBREL and PUBCOMP
that finish QoS 2 are exchanged by `rumqttc` itself.

MQTT has no negative acknowledgement. A QoS 1 or QoS 2 delivery the service never acknowledges
stays with the server, which sends it again only when a persistent session resumes on a new
connection, marked as a duplicate. [Sessions](sessions.md) draws that resumption, and
[Settlement](../settlement.md) what each outcome of a handler sends.

The client announces a Receive Maximum when it connects: how many QoS 1 and QoS 2 deliveries it
takes without acknowledging them. Once that many are outstanding, the server sends no more QoS 1
or QoS 2 messages on the connection until one is acknowledged. This crate announces 1000 by
default, and [`receive_maximum`](../../components/broker/limits.md) changes it. The window is the
bound on what a slow subscription has waiting for it. QoS 0 deliveries take no part in it.

A publish from the service returns once `rumqttc` has taken the message onto its request channel,
which holds 64 requests, not once the server has acknowledged it. Each QoS 1 and QoS 2 publish
is kept until the server acknowledges it. After a reconnect that resumes the session, `rumqttc`
sends them again the way the protocol says. After a reconnect that starts a new session, the
connection task sends them again as new publishes. The server cannot tell a message it already
received, so such a message may arrive twice, at QoS 2 too.

The flows are the protocol's own, from sections 4.3 and 4.9 of the
[MQTT 5.0 standard](https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html).
