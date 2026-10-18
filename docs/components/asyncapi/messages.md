---
sidebar_position: 3
title: Messages
description: "Each message reports the MQTT 5 properties it rides: a delivery the response topic and correlation data, a published message the correlation data."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Messages

Each message reports the MQTT 5 properties it rides: a delivery the response topic and correlation data, a published message the correlation data.

The crate carries two [headers](../publishing/headers.md) in the protocol's own properties: `correlation-id` in Correlation Data and `reply-to` in Response Topic.
The message binding describes them, each as a string.
A message a subscription reads reports both.
A requester sets the response topic on its own request, and a message a service publishes is not that request.
So a message an `Out` slot or a reply sends reports the correlation data alone.
Where a reply goes is the operation's `reply` object, and a reply named per request reports its [address](./replies.md) there.

The payload format indicator stays out of the binding.
It follows the media type one message carries in its `content-type` header, which no declaration knows.
The document reports the media type of the position's codec in the `contentType` the core fills.

## Example

`Telemetry` reports `correlationData` and `responseTopic`; `Alert`, which the slot publishes, reports `correlationData` only.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/mqtt_asyncapi.rs#messages"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/mqtt_asyncapi.rs#messages"
```

</TabItem>
</Tabs>
