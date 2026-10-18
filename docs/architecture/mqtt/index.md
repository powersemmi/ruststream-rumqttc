---
title: Topics
sidebar_position: 0
description: "An MQTT server delivers a message to every subscription whose filter matches its topic, to one member of a share group, and keeps retained messages."
---

import {Diagram} from '@site/src/components/Diagrams';

export const words = {
  "title": "One publish to devices/dev42/telemetry: the server copies it to each matching subscription, to one member of a share group, and keeps it when it is retained",
  "text": "The diagram as text",
  "publisher": "A publisher sends to the topic",
  "server": ["MQTT server", "matches the", "topic against", "the filter of", "every", "subscription"],
  "exact": "the same topic",
  "one": "+ is one level",
  "rest": "# is the rest",
  "other": "no match",
  "share": "Share group workers",
  "member1": "member 1 gets it",
  "member2": "member 2 does not",
  "retained": ["Retained message", "the last one of the topic"],
  "later": ["A subscription", "that joins later"],
  "dropped": ["A message no subscription matches is dropped,", "unless it is retained"],
  "publishes": "PUBLISH",
  "keeps": "retain flag set"
};

# Topics

An MQTT server delivers a message to every subscription whose filter matches its topic, to one
member of a share group, and keeps retained messages.

<Diagram name="topics" words={words} />

A topic is a string of levels separated by `/`. A subscription names a topic filter, which may hold
wildcards. `+` stands for exactly one level. `#` stands for the rest of the topic, its parent level
included, and comes last. A publish always names a full topic: this crate refuses a publish to a
topic with a wildcard before anything is sent.

A topic that starts with `$`, such as the server's own `$SYS/...`, is never matched by a filter that
starts with a wildcard. A service reads such topics only by naming their first level.

The server sends a message to each subscription that matches it. When one connection holds two
filters that both match, MQTT 5 lets the server send one packet per subscription, each naming that
subscription's identifier. Mosquitto does so, and this crate tells the copies apart by that
identifier, as [Demultiplexing](../demultiplexing.md) draws.

A [share group](../../components/subscriptions/shared.md) shares the work. The filter
`$share/workers/devices/+/telemetry` subscribes to `devices/+/telemetry` in the group `workers`,
and the server hands each message to one session of the group. A subscription outside the group
still gets its own copy.

A publish with the retain flag set stays on its topic. The server keeps the last
[retained message](../../components/publishing/retained.md) of each topic and sends it to every
new subscription whose filter matches, except a share group. A retained publish with an empty
payload clears the topic.

What a server keeps is that retained message per topic and the unacknowledged messages of a
persistent session, which [Sessions](sessions.md) draws. A message that arrives while nothing
matches it, and is not retained, is gone.

A message carries headers as MQTT 5 properties. A user property is a pair of UTF-8 strings, and the
content type, the response topic and the correlation data are properties of their own. This crate
maps headers onto them, as [Headers](../../components/publishing/headers.md) shows. An MQTT string
must not hold U+0000, and a server may close the connection on other control characters, so the
crate refuses a header or a topic that carries one.

The model is the protocol's own, from the
[MQTT 5.0 standard](https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html): topic names
and filters in section 4.7, shared subscriptions in 4.8.2, retained messages in 3.3.1.3. How a
delivery is acknowledged is on [Quality of service](qos.md).
