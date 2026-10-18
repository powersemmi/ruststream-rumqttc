---
sidebar_position: 7
description: "Answer MQTT 5 requests on the response topic each one names, with its correlation data, and fall back to a fixed topic for a request that names none."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Request and response

This guide answers MQTT 5 requests: each answer goes to the topic its request names and carries the request's correlation data.

MQTT 5 gives a request two properties for this.
The response topic says where the answer goes.
The correlation data is an opaque value the requester uses to match an answer to its question.
The broker maps them onto the `reply-to` and `correlation-id` headers in both directions.

The example is a device agent.
Consoles ask it for one setting on `devices/dev42/config/get`, and it answers with the setting's value.

## The responder

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust title="src/main.rs" reference="examples/guide/request_response.rs#responder"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust title="src/main.rs" reference="examples/manual/guide/request_response.rs#responder"
```

</TabItem>
</Tabs>

A [responder](../components/publishing/responders.md) is an ordinary handler with a reply: the value it returns is published.
`ConfigValue` names no topic, so the mount site names one: `reply("devices/dev42/config")`.
That topic is the fallback, for a request that carries no response topic.
A setting the device does not have ends in `drop`, and nothing is published.

## Answering on the response topic

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust title="src/main.rs" reference="examples/guide/request_response.rs#transform"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust title="src/main.rs" reference="examples/manual/guide/request_response.rs#transform"
```

</TabItem>
</Tabs>

`ToResponseTopic` is a publish transform on the reply position.
It reads the request's headers: `reply-to` becomes the answer's topic, and `correlation-id` is copied onto the answer.
It declares `Destination = Names`, which gives it the right to choose the topic.
A request without `reply-to` keeps the mount site's topic.

The [AsyncAPI document](../components/asyncapi/replies.md) of the service reports the reply's address as the request's `reply-to` header, `$message.header#/reply-to`, because no fixed channel holds the answers.

## Asking from the command line

The requester is any MQTT 5 client.
Mosquitto ships one, `mosquitto_rr`, which publishes a request with a response topic and prints the answer:

```bash
docker compose exec mosquitto mosquitto_rr \
  -t devices/dev42/config/get -e consoles/c1/responses -m '{"key":"firmware"}'
```

It prints `{"key":"firmware","value":"2.1.0"}`.
A requester inside a RustStream service publishes the request with both headers set, and reads the answers with a subscriber on its own response topic.

## Testing the responder

The tests in the example file publish a query with `reply-to` and `correlation-id` headers, in process.
The headers reach the handler through the same mapping onto MQTT 5 properties as on the wire.
The tests check the answer on `consoles/c1/responses` with its correlation data, the fallback topic for a query without a response topic, and that an unknown setting gets no answer.
