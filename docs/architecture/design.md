---
title: Design
sidebar_position: 2
description: "Where ruststream-rumqttc sits: between the RustStream runtime and the rumqttc client, on one connection a task of its own drives."
---

import {Diagram} from '@site/src/components/Diagrams';

export const words = {
  "title": "The service, the RustStream runtime, this crate, the rumqttc client and the MQTT server, top to bottom",
  "text": "The diagram as text",
  "service": "Your service",
  "handlers": ["#[subscriber]", "handlers"],
  "app": ["RustStream::new(..)", ".with_broker(..)"],
  "core": "RustStream core",
  "runtime": "Runtime: dispatch, lanes, settlement, retries",
  "crate": "ruststream-rumqttc",
  "subscriber": ["MqttSubscriber", "yields MqttMessage"],
  "publishers": ["MqttPublisher", "from a policy"],
  "context": "Context: DeliveryTopic",
  "inProcess": "In-process transport",
  "broker": ["MqttBroker", "one connection"],
  "descriptors": ["MqttTopic,", "MqttFilter"],
  "task": ["Connection task:", "routes each PUBLISH"],
  "client": "rumqttc",
  "eventLoop": ["EventLoop", "reads and writes"],
  "asyncClient": ["AsyncClient", "takes requests"],
  "server": "MQTT server",
  "mounted": "mounted",
  "polls": "polls the stream",
  "drives": "polls"
};

# Design

Where `ruststream-rumqttc` sits: between the RustStream runtime and the `rumqttc` client, on one
connection a task of its own drives.

<Diagram name="design" words={words} />

The crate implements the core's broker traits over
[`rumqttc`](https://docs.rs/rumqttc/0.25.1/rumqttc/v5/), speaking MQTT 5 only. The runtime above it
is the same for every broker. What is particular to MQTT is in this crate: the
[descriptors](../components/subscriptions/index.md), [what an ack sends](../components/settlement/index.md),
[where a retry copy goes](../components/settlement/retries.md), the
[topic of a delivery](../components/context/index.md) and the
[mapping of headers](../components/publishing/headers.md) onto MQTT 5 properties. Headers ride the
protocol's own properties, so a peer written in any language reads a plain MQTT message.

`rumqttc` splits a connection in two. The `AsyncClient` is a cloneable handle that queues requests:
a subscribe, a publish, an acknowledgement. The `EventLoop` writes those requests to the socket and
reads what the server sends, and it does both only while something polls it. Polling is also what
sends the keep-alive pings and what moves acknowledgements.

A connected broker holds one such pair. The crate polls the event loop on one task of its own,
spawned at connect, which does nothing but poll and route. A slow handler therefore never stalls
the connection: the pings, the acknowledgements and the other subscriptions' deliveries keep
moving. Subscribing, publishing and acknowledging go through the `AsyncClient` from whichever task
makes the call.

The connection task hands each PUBLISH the server sends to the subscriptions it belongs to, one
channel per subscription. Every subscription and publisher of the broker shares the one
connection, so a service with many handlers holds one session on the server.
[Demultiplexing](demultiplexing.md) draws how one connection tells its subscriptions apart.

How the pieces start and stop is on [Lifecycle](lifecycle.md), and the way of one message on
[Path](path.md).

## The in-process transport

With the `testing` feature, [`TestApp::start`](../components/testing/index.md) connects the same
broker to an in-process transport instead of a server. The descriptors, the policies and the
deliveries are the production ones, and the transport reads the broker's own settings. A publish
goes through the same validation and the same mapping of headers onto properties as on the wire.

The transport models the server this connection talks to. It matches filters with `+` and `#` and
keeps the `$` rule. It sends one packet per matching subscription at the lesser quality of service,
naming the subscription's identifier, and every packet goes through the same demultiplexing the
connection task runs. So a share group takes one copy between its members, and overlapping filters
each receive their own. It keeps the retained message of each topic, discards a message larger
than `max_packet_size`, and refuses an acknowledgement after shutdown.

What belongs to a server and a second connection runs only [live](../components/testing/live.md):
a persistent session and its redelivery on resume, the last will, the receive-maximum window, the
QoS 1 and QoS 2 handshakes, access control, and several connections on one server. Each broker
connected in process is a server of its own.

Without the feature the transport is not compiled. A production build's connection handle is
exactly the size of the `AsyncClient` it wraps, and the compiler checks that size.
