---
sidebar_position: 4
title: Replies
description: "A reply named per request has no fixed channel, so the document points its address at the reply-to header of the request."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Replies

A reply named per request has no fixed channel, so the document points its address at the `reply-to` header of the request.

A [responder](../publishing/responders.md) answers on the response topic its request carries.
The crate maps that property onto the `reply-to` header, and a publish transform with `Destination = Names` reads it there.
The document is built before any request arrives, so it cannot name the topic.
The operation's `reply.address.location` is `$message.header#/reply-to` instead: a client reads the address of its answer from the header of its own request.
The mount-site name, `reply("devices/dev42/pong")`, is the reply channel the document reports, and where a request without a response topic is answered.
A reply that the type or the mount site names reports that channel alone.

## Example

The operation `receive_devices_dev42_ping` reports `"location": "$message.header#/reply-to"` in its reply address.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/mqtt_asyncapi.rs#replies"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/mqtt_asyncapi.rs#replies"
```

</TabItem>
</Tabs>
