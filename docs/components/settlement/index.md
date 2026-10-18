---
title: Acknowledgement
description: "An ack sends PUBACK at QoS 1 and PUBREC at QoS 2; QoS 0 has no acknowledgement, and MQTT has no negative one."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Acknowledgement

An [`ack`](api:ruststream::runtime::HandlerOutcome::ack) sends PUBACK at QoS 1 and PUBREC at QoS 2; QoS 0 has no acknowledgement, and MQTT has no negative one.

A message settles when the handler returns, and the delivery's own quality of service decides what that sends.
A delivery arrives at the lower of the publisher's QoS and the subscription's, so a QoS 1 subscription still receives a QoS 0 publish at QoS 0.
At QoS 1 and 2 the server keeps a delivery until it is acknowledged; the client finishes the QoS 2 handshake on its own.
A QoS 0 delivery has no acknowledgement packet, so settling it reports [`AckError::Unsupported`](api:ruststream::AckError::Unsupported), which the runtime logs as a warning.
[`drop`](api:ruststream::runtime::HandlerOutcome::drop) acknowledges the delivery too, because an acknowledgement is the only final answer MQTT has; the server never sends the message again.
[`retry`](./retry.md) asks for a negative acknowledgement, which MQTT does not have, so the delivery stays unacknowledged.
[`retry_after`](./retry-after.md) is the outcome that retries within the session.
In the manual form `Ok` acknowledges and `Err(outcome)` settles by the outcome.
When the server sends one packet for two overlapping filters, the acknowledgement belongs to one delivery, and settling the other reports `Unsupported`.

## Example

A device agent applies the commands it knows and drops the rest; both answers settle the delivery.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/mqtt_settlement_ack.rs#handler"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/mqtt_settlement_ack.rs#handler"
```

</TabItem>
</Tabs>
