---
sidebar_position: 6
title: Batches
description: "A slice handler reads batches the crate assembles on the client: a batch closes at the size the mount site names or 20 ms after its first delivery."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Batches

A slice handler reads batches the crate assembles on the client: a batch closes at the size the mount site names or 20 ms after its first delivery.

A handler over `&[T]` is a batch handler, and [`batch(n)`](api:ruststream::runtime::SubscriberSettings::batch) at the mount site is its largest batch.
An MQTT packet carries one message, so the crate collects deliveries into batches on the client.
A batch closes when it holds `n` deliveries, or 20 milliseconds after its first delivery, whichever comes first.
The size is the mount site's, and the 20 ms deadline is the crate's.
A subscription with nothing to read waits for its first delivery as long as it takes.
Nothing at the mount site says which side of the wire filled a batch, so a batch handler written for another broker mounts here unchanged.

One outcome settles every delivery of the batch, and a `Vec<HandlerOutcome>` settles delivery `i` with outcome `i`.
Each delivery is [acknowledged](../settlement/index.md) on its own.
A batch abandoned while it fills leaves its deliveries unacknowledged.
In the manual form the body is an `impl Handle<[T]>`, where `Ok(())` acks the batch and `Err(outcomes)` settles delivery by delivery.

## Example

The readings of a share group, stored up to 64 at a time.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/mqtt_batches.rs#batches"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/mqtt_batches.rs#batches"
```

</TabItem>
</Tabs>
