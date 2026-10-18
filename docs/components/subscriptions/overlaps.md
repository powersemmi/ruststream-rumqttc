---
sidebar_position: 5
title: Overlaps
description: "Overlapping filters on one connection each receive a message once, told apart by subscription identifiers; one filter mounted twice shares it."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Overlaps

Overlapping filters on one connection each receive a message once, told apart by subscription identifiers; one filter mounted twice shares it.

Two filters overlap when one topic can match both, such as `devices/+/telemetry` and `devices/dev42/#`.
They are two subscriptions to the server, and each receives a message once.
The server sends a packet per matching subscription, and the crate tells the packets apart by MQTT 5 subscription identifiers.
The crate attaches an identifier only to a filter that meets another one on the same connection.
A share group next to a plain subscription on the same filter overlaps the same way.

The same filter mounted twice, or two members of one share group, is one subscription to the server.
The crate hands its deliveries to the mounts in turn, and the filter stays subscribed until the last of them is dropped.

A server announces whether it offers subscription identifiers when the connection opens.
On a server without them, a filter that meets another one on the same connection does not open, and the error names both filters.
Two brokers with client ids of their own keep such filters on separate connections.

## Example

The fleet's readings and everything dev42 sends: a reading of dev42 reaches both handlers, once each.
Each retry topic matches its own filter only, so a [retry copy](../settlement/out-retry.md) returns to the subscription that asked for it.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/mqtt_subscriptions_overlaps.rs#handlers"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/mqtt_subscriptions_overlaps.rs#handlers"
```

</TabItem>
</Tabs>
