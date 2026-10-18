---
sidebar_position: 1
description: "Install ruststream-rumqttc, start a local Mosquitto, run a first subscriber against it and test it without a server."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Getting started

This guide installs `ruststream-rumqttc`, starts a local MQTT server and runs a first service against it.

## Installation

```toml title="Cargo.toml"
[dependencies]
ruststream = { version = "0.7", features = ["macros", "json", "logging"] }
ruststream-rumqttc = "0.7"
serde = { version = "1", features = ["derive"] }
tracing = "0.1"

[dev-dependencies]
ruststream-rumqttc = { version = "0.7", features = ["testing"] }
tokio = { version = "1", features = ["macros", "rt"] }
```

`ruststream` is the framework: `macros` gives the attributes, `json` the codec, and `logging` prints the service's log to the terminal.
`ruststream-rumqttc` is the MQTT 5 broker.
The broker enables no codec of its own, so the service picks `json`, `msgpack` or `cbor` on `ruststream`.

The broker crate has two features, both off by default:

| Feature | What it gives |
|---|---|
| `testing` | the [in-process MQTT server](../components/testing/index.md) behind `TestApp` |
| `asyncapi` | the [MQTT bindings](../components/asyncapi/index.md) in the generated AsyncAPI document |

## System requirements

The `rumqttc` client speaks TLS through rustls, whose cryptography comes from `aws-lc-rs`.
`aws-lc-rs` compiles C code, so the build needs a C compiler.
On Debian and Ubuntu, `build-essential` provides it.
Other platforms are listed in the [aws-lc-rs requirements](https://aws.github.io/aws-lc-rs/requirements/index.html).

## A local MQTT server

One Eclipse Mosquitto container is enough to develop against.
It reads its listeners from a configuration file:

```text title="mosquitto.conf"
listener 1883
allow_anonymous true
```

```yaml title="compose.yaml"
services:
  mosquitto:
    image: eclipse-mosquitto:2
    ports:
      - "1883:1883"
    volumes:
      - ./mosquitto.conf:/mosquitto/config/mosquitto.conf:ro
```

```bash
docker compose up -d
```

The server listens on `localhost:1883` and accepts clients without a password.

## The first service

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust title="src/main.rs" reference="examples/guide/getting_started.rs#service"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust title="src/main.rs" reference="examples/manual/guide/getting_started.rs#service"
```

</TabItem>
</Tabs>

`record` reads the JSON telemetry of every device.
It acknowledges a reading and drops one below absolute zero.
[`MqttFilter`](../components/subscriptions/mqtt-filter.md) subscribes to a topic filter: `+` stands for one level of the topic, here the device's id.
[`Qos::AtLeastOnce`](../components/subscriptions/qos.md) makes each reading an acknowledged delivery: the acknowledgement leaves when `record` returns.
A filter matches many topics, so the mount site names the topic a [delayed retry](../components/settlement/out-retry.md) goes to; every subscription over a filter names one, and the service refuses to start without it.

[`MqttBroker::new`](../components/broker/index.md) takes the server's URL and the client id, and only records them.
The connection opens when the service starts.
The client id names the service's session on the server, so each running process has its own.
`#[ruststream::app]` turns `app` into `main`; in the manual form, [`run_main`](api:ruststream::runtime::cli::run_main) does the same.

Run the service:

```bash
cargo run -- run
```

Publish a reading from another terminal with the client that ships in the image:

```bash
docker compose exec mosquitto mosquitto_pub -V mqttv5 -q 1 \
  -t devices/dev42/telemetry -m '{"device":"dev42","temperature":21.5}'
```

The service logs `reading recorded` with the device and its temperature.
<kbd>Ctrl</kbd>+<kbd>C</kbd> stops it.

## The first test

The example file also holds two tests.
Each starts the same `app` with `TestApp::start`, publishes a reading to `devices/dev42/telemetry` and checks how `record` settled it.
`MqttBroker` connects to an in-process MQTT server there, so the tests need no Mosquitto:

```bash
cargo test
```

[Testing an MQTT service](testing.md) walks through the tests and what the in-process server models.

## Coming from rumqttc

A service on rumqttc drives the event loop itself and acknowledges each message by hand.
Here is where each part goes in RustStream:

| rumqttc | RustStream |
|---|---|
| `MqttOptions` and its setters | the [`MqttBroker`](../components/broker/index.md) builder: [`keep_alive`](../components/broker/limits.md), [`credentials`](../components/broker/security.md), [`clean_start`](../components/broker/session.md), [`last_will`](../components/broker/last-will.md) |
| an `EventLoop::poll` loop | the broker's own connection task, which reconnects and resubscribes |
| `AsyncClient::subscribe` | a `#[subscriber]` handler on an [`MqttTopic`](../components/subscriptions/mqtt-topic.md) or an [`MqttFilter`](../components/subscriptions/mqtt-filter.md) |
| deserializing `publish.payload` | the handler's parameter type, decoded by the codec |
| `set_manual_acks(true)` and `AsyncClient::ack` | the handler's outcome ([Delivery guarantees](delivery.md)) |
| `AsyncClient::publish` | a reply or an `Out` parameter, sent through a [publish policy](../components/publishing/index.md) ([Publishing commands](publishing.md)) |
