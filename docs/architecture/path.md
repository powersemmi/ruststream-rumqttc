---
title: Path
sidebar_position: 4
description: "The way of one MQTT message through a RustStream service: the event loop, the connection task, the delivery, the handler, the reply and the acknowledgement."
---

import {Diagram} from '@site/src/components/Diagrams';

export const words = {
  "title": "The path of one message that is answered: down the left column from the server to the handler, up the right column back onto the wire",
  "text": "The diagram as text",
  "poll": ["EventLoop::poll", "decodes the PUBLISH"],
  "demux": ["Connection task", "finds the subscription"],
  "message": ["MqttMessage", "payload shared, no copy"],
  "dispatch": ["Dispatch", "one loop, or lanes"],
  "decode": ["Decode", "read from the payload"],
  "handler": ["Handler", "async fn, #[subscriber]"],
  "server": ["MQTT server"],
  "write": ["EventLoop writes", "PUBLISH, then PUBACK"],
  "requests": ["Request channel", "of the AsyncClient"],
  "ack": ["Acknowledge", "PUBACK or PUBREC"],
  "publisher": ["MqttPublisher", "hands the buffer over"],
  "encode": ["Reply", "encoded into a buffer"],
  "then": "then"
};

# Path

The way of one MQTT message through a RustStream service: the event loop, the connection task, the
delivery, the handler, the reply and the acknowledgement.

<Diagram name="path" words={words} />

The connection task polls the `rumqttc` event loop, which reads the PUBLISH from the socket and
decodes it. The task reads the topic, finds the subscriptions the packet belongs to, as
[Demultiplexing](demultiplexing.md) draws, and sends each its delivery over a channel of its own.
A topic that is not UTF-8 is dropped with a warning.

The [`MqttMessage`](api:ruststream_rumqttc::MqttMessage) shares the packet's payload, so the body
is never copied. It owns a copy of the topic and the headers built from the packet's properties.
A QoS 1 or QoS 2 delivery also keeps the packet's identity, which its acknowledgement needs. The
[context](../components/context/index.md) a handler reads holds the topic as well.

A subscription's channel has no bound of its own. For QoS 1 and QoS 2 the bound is the server's:
it sends no more than the [Receive Maximum](mqtt/qos.md) unacknowledged deliveries. A packet that
matches no open subscription waits, up to 1024 of them, for a subscription that opens later.

The handler's reply is encoded into a buffer, and the
[publisher](../components/publishing/index.md) hands that buffer to `rumqttc` as it is, without a
copy. The headers become the packet's properties. The publish returns once the request is on the
client's request channel, not once the server has acknowledged it.

The delivery settles after the reply. Its acknowledgement goes through the same request channel,
so it leaves after the reply and before a later shutdown's DISCONNECT. What each outcome sends is on
[Settlement](settlement.md).

The [benchmarks](benchmarks/index.md#cost-of-the-code) count this path: the instructions and
allocations of one delivery and one reply, the client's work on the service's thread included.
