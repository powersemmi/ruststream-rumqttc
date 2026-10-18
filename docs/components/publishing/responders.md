---
sidebar_position: 4
title: Responders
description: "A responder answers on the response topic a request carries, through a reply transform that also copies the correlation data."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Responders

A responder answers on the response topic a request carries, through a reply transform that also copies the correlation data.

An MQTT 5 request carries the topic its answer goes to in the response topic property, and the handler reads it as the `reply-to` [header](./headers.md).
Its correlation data arrives as the `correlation-id` header, and the requester matches the answer to its question by it.
A [`PublishTransform`](api:ruststream::runtime::PublishTransform) on the reply position reads both from the request: `cx.headers().reply_to()` and `cx.headers().correlation_id()`.
It names the answer's topic, so it declares `type Destination = Names`.
The reply type declares no topic of its own, which is what leaves the naming to the transform.
The topic the mount site names, `reply("..")` in the macro form, is the fallback: a request without a response topic is answered there.
The generated document points the reply's address at the request's `reply-to` header, as [Replies](../asyncapi/replies.md) shows.

## Example

A device agent answers each status request on the response topic of the console that asked:

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/mqtt_publishing_responders.rs#responder"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/mqtt_publishing_responders.rs#responder"
```

</TabItem>
</Tabs>
