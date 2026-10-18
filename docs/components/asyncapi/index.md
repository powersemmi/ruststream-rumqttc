---
title: MQTT bindings
description: "With asyncapi the generated document speaks MQTT 5: the session the client opens, the QoS of each operation and the properties of each message."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# MQTT bindings

With the `asyncapi` feature the generated document speaks MQTT 5: the session the client opens, the QoS of each operation and the properties of each message.

The core builds the AsyncAPI document from the app, before anything connects.
This crate adds the specification's `mqtt` binding, version `0.2.0`, to the parts it knows about.
A [server](./server.md) reports where the client connects, the protocol version `5` and the session it opens.
An [operation](./operations.md) reports the quality of service it runs at.
A [message](./messages.md) reports the MQTT 5 properties it rides.
A reply named per request reports where its [address](./replies.md) is read.

Everything comes from the service's own declarations: the broker, the descriptors, the publish policies and the transforms.
The document is published and shared, so no binding carries a credential.
The binary `#[ruststream::app]` writes prints the document with `asyncapi gen`.

## Example

The dependency line:

```toml
ruststream-rumqttc = { version = "0.7", features = ["asyncapi"] }
```

The service registers its broker under the label `mqtt`, the key of its server in the document.
`cargo run --example mqtt_asyncapi --features asyncapi -- asyncapi gen` prints the document.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/mqtt_asyncapi.rs#app"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/mqtt_asyncapi.rs#app"
```

</TabItem>
</Tabs>
