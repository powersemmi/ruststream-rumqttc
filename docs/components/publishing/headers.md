---
sidebar_position: 3
title: Headers
description: "Headers travel as MQTT 5 user properties; content-type, reply-to and correlation-id take the protocol's own properties."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Headers

Headers travel as MQTT 5 user properties; `content-type`, `reply-to` and `correlation-id` take the protocol's own properties.

A peer in any language reads them as plain MQTT, with no envelope around the payload.
`content-type` becomes the content type property, `reply-to` the response topic a [responder](./responders.md) answers on, and `correlation-id` the correlation data, in both directions.
A message with no headers is published with no properties at all.
A textual content type (`application/json`, any `text/` type, any `+json` type) sets the payload format indicator to 1, which tells a peer the body is UTF-8; any other sets it to 0.
A property is an MQTT string, and a server may close the connection on one with a control character.
So a header name or value that is not UTF-8, or carries a control character or a Unicode non-character, refuses the publish with [`MqttError::Publish`](../broker/errors.md).
The codec of a position writes no header, so nothing sets the content type unless the service does.
A [publish transform](api:ruststream::runtime::PublishTransform) sets it at the mount site, once for every message of one position: the replies, a slot or the retry copies.

## Example

Every reply of the position states that its body is JSON:

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/mqtt_publishing_headers.rs#transform"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/mqtt_publishing_headers.rs#transform"
```

</TabItem>
</Tabs>
