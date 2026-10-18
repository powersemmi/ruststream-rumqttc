---
sidebar_position: 5
description: "Send a command to one device out of many, choose the quality of service per message, and carry headers as MQTT 5 properties that peers in any language read."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Publishing commands

This guide sends commands to one device out of many, at the quality of service each command needs, with headers that a device written in any language reads.

The example is a commander service.
Operators send a command request to `operators/commands`.
The service forwards each command to the topic of its device, `devices/<id>/commands`.

## A topic per device

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust title="src/main.rs" reference="examples/guide/publishing.rs#command"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust title="src/main.rs" reference="examples/manual/guide/publishing.rs#command"
```

</TabItem>
</Tabs>

The topic of `DeviceCommand` is a name template.
`{device}` becomes a setter of the publish, so the handler names the device and the topic follows.
`Commands` is the slot the handler publishes through, and it publishes `DeviceCommand` only.

## The command

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust title="src/main.rs" reference="examples/guide/publishing.rs#dispatch"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust title="src/main.rs" reference="examples/manual/guide/publishing.rs#dispatch"
```

</TabItem>
</Tabs>

The slot's policy gives every command its quality of service.
An urgent command takes the [`qos` step](../components/publishing/mqtt-publish-steps.md) and goes out at `QoS` 2 instead.
A step changes one message; a setting the call does not name is the policy's.
The step writes [`MqttPublishOptions`](api:ruststream_rumqttc::MqttPublishOptions), so the slot's bound names that type, and the file imports the broker's prelude.
`retain` is the other step, and [Device state](device-state.md) uses it as a policy.

The `operator` header travels as an MQTT 5 [user property](../components/publishing/headers.md).
A device reads it as a plain property, with no envelope around the payload.
The headers `content-type`, `reply-to` and `correlation-id` take the protocol's own properties instead: the content type, the response topic and the correlation data.

`publish` returns once the client holds the message, before the server confirms it.
At `QoS` 1 and 2 the client sends it again until the server acknowledges it, across reconnects.
An error from `publish` means the client did not take the message: the connection is shut down, or the message breaks the protocol's rules.
A topic with `+` or `#` breaks them: the request names the device `+`, the topic becomes `devices/+/commands`, and `dispatch` drops the request.
A header or a topic with a control character is refused the same way, because a server may close the connection on one.

## The content type

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust title="src/main.rs" reference="examples/guide/publishing.rs#app"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust title="src/main.rs" reference="examples/manual/guide/publishing.rs#app"
```

</TabItem>
</Tabs>

The mount site gives the slot its policy, `QoS` 1, and a publish transform.
`Json` puts `content-type: application/json` on every command the slot sends.
The broker turns it into the content type property and sets the payload format indicator, so a device reads the body as UTF-8 text.
The codec does not write the header itself: a transform at the mount site is where a service states its media type once.

## Testing the commands

The tests in the example file run the app in process and read the commands the slot published.
`tb.out::<Commands>()` shows the per-message settings: none for a routine command, `QoS` 2 for an urgent one.
`published` on `devices/dev42/commands` shows the payload and both headers.
The in-process server validates topics and headers the way the client does, so the request for device `+` is dropped there too.
