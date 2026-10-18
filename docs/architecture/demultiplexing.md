---
title: Demultiplexing
sidebar_position: 5
description: "One connection carries every subscription: subscription identifiers tell overlapping filters apart, and subscriptions on one filter take turns."
---

import {Diagram} from '@site/src/components/Diagrams';

export const words = {
  "title": "Two PUBLISH packets for one topic, each naming the subscription identifier of the filter it was sent for, handed to that filter's subscriptions in turn",
  "text": "The diagram as text",
  "registry": "Filters on the connection",
  "identifier": "identifier",
  "first": ["takes it", "(its turn)"],
  "second": ["takes the", "next one"],
  "only": ["one subscription", "takes every one"],
  "next": "next",
  "held": ["A packet no open subscription claims waits, up to 1024,", "for the subscription that opens later"]
};

export const lanes = {
  "title": "One subscription's dispatch: one stream of deliveries spread over two lanes in turn",
  "text": "The diagram as text",
  "subscription": ["The subscription", "a topic or a filter, on the shared connection"],
  "stream": ["MqttSubscriber stream", "one delivery at a time"],
  "pool": ["workers(2)", "the lanes take the deliveries in turn"],
  "lane0": ["Lane 1", "runs its deliveries in order"],
  "lane1": ["Lane 2", "runs its deliveries in order"],
  "noKey": ["An MQTT delivery carries no key:", "under by_key too, the deliveries rotate over the lanes"],
  "scale": ["Across processes: a share group", "hands each message to one member"],
  "delivered": "delivered",
  "oneAtATime": "one at a time"
};

# Demultiplexing

One connection carries every subscription: subscription identifiers tell overlapping filters
apart, and subscriptions on one filter take turns.

<Diagram name="demultiplexing" words={words} />

The server subscribes a filter once per connection. Two handlers on the same filter, or the two
members of a share group in one service, are one subscription on the server and two subscriptions
in the service. The crate hands the filter's deliveries to them in turn. The filter stays
subscribed while any of them is open, and a shutdown leaves it in the session. It is subscribed at
the highest quality of service any of them asks for.

Filters that differ but can match one topic, `devices/+/telemetry` next to `devices/#` or a share
group next to a plain subscription on its filter, are separate subscriptions on the server. For a
message that matches both, Mosquitto sends a packet for each. MQTT 5 lets a subscription carry an
identifier, and the server names it in every packet it sends for that subscription. The crate gives
an identifier only to a filter that [overlaps](../components/subscriptions/overlaps.md) another one
on the connection, and routes each packet by the identifier it names.

A filter that meets another one only after it was subscribed is subscribed again, with an
identifier, and asks the server for no retained messages that time, so a retained message is not
delivered twice. The identifier is derived from the filter itself. A restarted service that
resumes a persistent session therefore gives each filter the identifier the server still holds.

Subscription identifiers are optional for a server, and its CONNACK says whether it offers them.
On a server that does not, opening a filter that overlaps another one on the same connection fails
with an error naming both, before anything is subscribed. Two brokers with client ids of their own
keep such filters apart. When a server sends one packet for two filters, the delivery to the first
carries the acknowledgement, and the other's `ack` reports `AckError::Unsupported`.

A packet no open subscription claims is held, because a resumed session sends its backlog before
the service has opened the subscriptions it belongs to. Up to 1024 packets wait, and the oldest is
dropped with a warning past that.

## Lanes

<Diagram name="lanes" words={lanes} />

Without `workers`, one loop runs the handler for each delivery of a subscription in turn. The order
in which the server sent them is kept, and nothing runs side by side.

`workers(n)` runs `n` lanes, and a lane runs its deliveries in order. An MQTT delivery carries no
partition key, so the lanes take the deliveries in turn, under `workers(n, by_key)` as well.
Deliveries of one topic then run side by side on different lanes.

Across processes, a [share group](../components/subscriptions/shared.md) divides the messages: the
server hands each one to one member. The [Receive Maximum](mqtt/qos.md) bounds how many QoS 1 and
QoS 2 deliveries a connection holds unacknowledged at once, across all its subscriptions.

A [batch handler](../components/subscriptions/batches.md) takes up to the size the mount site
names. A PUBLISH carries one message, so the crate assembles the batch on the client. A batch
closes when it holds that size or 20 milliseconds after its first delivery.
