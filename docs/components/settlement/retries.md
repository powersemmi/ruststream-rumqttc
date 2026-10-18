---
sidebar_position: 3
title: Retries
description: "max_attempts caps the deliveries one message gets, counted on the retry-count header, because MQTT counts no redeliveries."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Retries

[`max_attempts(n)`](api:ruststream::runtime::Mounting::max_attempts) caps the deliveries one message gets, counted on the retry-count header, because MQTT counts no redeliveries.

A redelivered PUBLISH packet carries a duplicate flag and no counter.
So the runtime counts on [`RETRY_COUNT_HEADER`](api:ruststream::runtime::RETRY_COUNT_HEADER), which travels on the copies [`retry_after`](./retry-after.md) publishes.
The cap counts deliveries, the first one included: `nonzero!(5u32)` gives a message five.
Under a cap every retry travels as such a copy, an immediate [`retry()`](./retry.md) too, so the count reaches the cap whichever outcome the handler returns.
The delivery that reaches the cap goes to the [dead-letter topic](./dead-letters.md) instead of coming back.
A cap without a dead-letter topic rejects the delivery, which on MQTT acknowledges it and lets it go; a warning names the subscription.
A filter subscription also names where its copies go, with [`out_retry`](./out-retry.md).

## Example

Every registration of the service gets five deliveries per message and a dead-letter topic of its own.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/mqtt_retries.rs#declaration"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/mqtt_retries.rs#declaration"
```

</TabItem>
</Tabs>
