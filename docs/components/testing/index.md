---
title: Testing
description: "TestApp::start runs the production app on an in-process MQTT server: wildcards, share groups, retained messages and QoS, with no broker."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Testing

[`TestApp::start`](api:ruststream::testing::TestApp::start) runs the production app on an in-process MQTT server: wildcards, share groups, retained messages and QoS, with no broker.

A test hands the harness the app `main` runs, unchanged, and addresses the broker by its production type, `tb.broker::<MqttBroker>()`.
The `testing` feature in `[dev-dependencies]` adds the in-process mode, and turns on the core's harness with it.

The in-process server has no settings of its own.
It reads the broker's, and checks them as the connect does: a keep-alive under five seconds or an `mqtts://` URL without `tls_ca` fails `TestApp::start`.
A publish goes through the same validation and the same mapping onto MQTT 5 properties as on the wire.

It routes as a server routes.
A publish reaches every subscription whose filter matches its topic, `+` and `#` included, and a filter starting with a wildcard skips the topics that start with `$`.
A share group hands each message to one of its members.
Two overlapping filters each receive their own copy, as a server sends them.
A delivery arrives at the lower of the publish and the subscription QoS, and a QoS 0 delivery has nothing to acknowledge.
A retained message reaches every later subscription outside a share group, and an empty retained payload clears it.
A message over the broker's `max_packet_size` never arrives, and a publish or an acknowledgement after shutdown is refused.

A publish through the harness returns once every handler it woke has settled, a reply included.
The assertions that follow read finished state.
What only a server has is for a [live test](./live.md): the login and TLS, a persistent session, the last will, the receive window, a lost connection, and several connections on one server.

## Example

The dependency line:

```toml
[dev-dependencies]
ruststream-rumqttc = { version = "0.7", features = ["testing"] }
```

The test publishes a hot reading to one device's topic, and reads how the handler settled it and the alert it published.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/mqtt_testing.rs#testapp"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/mqtt_testing.rs#testapp"
```

</TabItem>
</Tabs>

The filter `devices/+/telemetry` matches one level in place of the `+`, so a topic one level deeper reaches no handler.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/mqtt_testing.rs#routing"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/mqtt_testing.rs#routing"
```

</TabItem>
</Tabs>
