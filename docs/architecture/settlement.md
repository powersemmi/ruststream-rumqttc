---
title: Settlement
sidebar_position: 6
description: "What ack, drop, retry and retry_after become on MQTT: one acknowledgement at QoS 1 and 2, nothing at QoS 0, and copies the runtime publishes."
---

import {Diagram} from '@site/src/components/Diagrams';

export const table = {
  "title": "Each outcome of a handler on a QoS 1 or QoS 2 delivery: what the delivery sends and what follows",
  "text": "The diagram as text",
  "outcomeLane": "Handler",
  "wireLane": "Sent",
  "effectLane": "Then",
  "ack": ["ack()"],
  "drop": ["drop()"],
  "retry": ["retry()"],
  "retryCapped": ["retry()", "under a cap"],
  "retryAfter": ["retry_after(d)"],
  "unsettled": ["no outcome:", "a batch let go"],
  "nothing": "nothing",
  "ackEffect": ["finished"],
  "dropEffect": ["finished,", "the message", "is let go"],
  "retryEffect": ["back when a", "persistent session", "resumes"],
  "retryCappedEffect": ["a copy", "published now"],
  "retryAfterEffect": ["a copy", "published after d"],
  "unsettledEffect": ["as retry():", "back on resume"],
  "qos0": ["At QoS 0 nothing is sent: ack and drop report AckError::Unsupported,", "and a copy the runtime publishes still goes out"]
};

export const retries = {
  "title": "A retry on MQTT: the attempt cap, counted on the runtime's header, decides between a copy and the dead-letter topic",
  "text": "The diagram as text",
  "delivery": ["A delivery", "its attempt read from x-ruststream-retry-count"],
  "outcome": ["The handler returns", "retry_after(d), or retry() under a declaration"],
  "cap": ["max_attempts: is this attempt below the cap?"],
  "below": "Below the cap",
  "acked": ["The original is", "acknowledged,", "the copy waits d"],
  "topicCopy": ["MqttTopic:", "the copy goes", "to its topic"],
  "filterCopy": ["MqttFilter:", "to the topic", "the mount site or", "a transform names"],
  "atCap": "At the cap",
  "deadLetter": ["Dead-letter topic:", "the delivery", "published there"],
  "rejected": ["No dead letter:", "acknowledged", "and let go"],
  "fanOut": ["A copy is a publish: every subscription whose filter", "matches its topic receives it. A copy waiting for its", "delay lives in the process, and a crash before the", "timer fires loses it."],
  "handled": "handled",
  "retries": "a retry",
  "belowCap": "below",
  "reached": "at the cap"
};

# Settlement

What `ack`, `drop`, `retry` and [`retry_after`](../components/settlement/retry-after.md) become on
MQTT: one acknowledgement at QoS 1 and 2, nothing at QoS 0, and copies the runtime publishes.

<Diagram name="settlement" words={table} />

MQTT has one answer to a delivery, the acknowledgement: PUBACK at QoS 1, PUBREC at QoS 2. `ack`
sends it. `drop` sends it too, because acknowledging is the only way the protocol lets a service
end a message.

MQTT has no negative acknowledgement, so the server never sends a message again on the live
connection. A [`retry()`](../components/settlement/retry.md) with nothing declared reports
`AckError::Unsupported`, which the runtime logs, and leaves the delivery unacknowledged. The server
sends it again only when a persistent session resumes on a new connection. Until then it holds one
slot of the [Receive Maximum](mqtt/qos.md) window: once the window is full of such deliveries, the
server sends no more QoS 1 or QoS 2 messages on the connection. A batch the handler lets go stays
unacknowledged the same way.

A retry within the session is a copy the runtime publishes. `retry_after(d)` acknowledges the
original, waits for `d` and publishes a copy. A `retry()` under a declared cap or dead-letter
topic publishes its copy at once. A QoS 0 delivery has nothing to acknowledge, and its copy goes
out all the same.

| Outcome | QoS 0 |
|---|---|
| `ack`, `drop` | `AckError::Unsupported` |
| `retry()` | `AckError::Unsupported`; under a declared cap, a copy published at once |
| `retry_after(d)` | a copy published after `d` |
| no outcome | nothing |

## Retries

<Diagram name="retries" words={retries} />

The cap is declared at the mount site with
[`max_attempts`](api:ruststream::runtime::Mounting::max_attempts), and the first delivery counts
as one. MQTT counts no deliveries of its own, so the runtime counts the copies it publishes, in
each copy's `x-ruststream-retry-count` header.

Below the cap, the copy goes to the address of the subscription's form. An
[`MqttTopic`](../components/subscriptions/mqtt-topic.md) addresses its own topic, a shared one
included, so its registration names nothing. An
[`MqttFilter`](../components/subscriptions/mqtt-filter.md) matches many topics and publishes to
none, so its registration [names the topic](../components/settlement/out-retry.md) a copy goes to,
once or per delivery through a transform. A filter registration that names neither does not start.

At the cap, the delivery goes to the [dead-letter topic](../components/settlement/dead-letters.md),
payload unchanged. With no dead-letter topic declared, it is acknowledged and let go.

A copy is an ordinary publish, so it reaches every subscription whose filter matches its topic,
in this service and in others. A dead-letter topic that a live filter matches hands the message
straight back, so it takes a name no subscription reads. A copy waiting for its delay lives in the
process: a graceful shutdown waits for it, and a crash before the timer fires loses it.
