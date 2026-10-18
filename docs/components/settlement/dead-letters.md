---
sidebar_position: 4
title: Dead letters
description: "dead_letter names the topic a delivery that reached its cap is published to; it lies outside every filter the service reads."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Dead letters

[`dead_letter(topic)`](api:ruststream::runtime::Mounting::dead_letter) names the topic a delivery that reached its cap is published to; it lies outside every filter the service reads.

It follows [`max_attempts(n)`](./retries.md) at the mount site.
When the `n`-th delivery asks for a retry, the runtime acknowledges it and publishes it to the dead-letter topic instead.
The message keeps its payload and its headers.
[`RETRY_COUNT_HEADER`](api:ruststream::runtime::RETRY_COUNT_HEADER) on it says how many times it was delivered.
The dead-letter topic is an ordinary topic, and a subscription of this service or of another one reads it.
A filter of the service that matches the dead-letter topic receives the spent message as a new delivery, so the topic stays outside every other filter the service reads.

## Example

Each registration sends its spent messages to a topic of its own under `dead/`:

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

The dead-letter topic is read by an ordinary subscription:

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/mqtt_retries.rs#dead_letter"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/mqtt_retries.rs#dead_letter"
```

</TabItem>
</Tabs>
