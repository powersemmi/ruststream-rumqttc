---
sidebar_position: 8
description: "Test an MQTT service in process with TestApp: the production app, wildcard topics, published alerts, delays on a paused clock, and the same test on Mosquitto."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Testing an MQTT service

A test runs the service's own app with [`TestApp`](api:ruststream::testing::TestApp), against an [in-process MQTT server](../components/testing/index.md) or against Mosquitto.

The service under test watches freezers.
Each freezer publishes its temperature to `freezers/<id>/temperature`, and the monitors share the readings in a share group.
Every reading goes into a temperature log.
A freezer warmer than -18 °C raises an alert on `freezers/alerts`.
When the log does not answer, the handler asks for the reading again in ten seconds with [`retry_after`](../components/settlement/retry-after.md).
After three deliveries a reading goes to `freezers/review`, the [dead-letter topic](../components/settlement/dead-letters.md).

The handler `watch` publishes the alert through an `Alerts` slot.
The app is built by `service(broker)`, so a test chooses the broker and `main` uses the production one.
The tests need the `testing` feature of `ruststream-rumqttc` in `[dev-dependencies]`.

## A warm freezer

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust title="src/main.rs" reference="examples/guide/testing.rs#in_process"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust title="src/main.rs" reference="examples/manual/guide/testing.rs#in_process"
```

</TabItem>
</Tabs>

[`TestApp::start`](api:ruststream::testing::TestApp::start) connects [`MqttBroker`](../components/broker/index.md) to an in-process MQTT server.
The URL is never dialled.
The test addresses the broker by its production type, `tb.broker::<MqttBroker>()`.
A `Reading` names no topic, so the test names one with `.to("freezers/f1/temperature")`, as a freezer would.
The handler's filter matches it the way Mosquitto would.
`publish` returns once the handler and its alert have settled.
So the next lines assert on finished state: how the subscription settled and what reached `freezers/alerts`.
The subscription is addressed by its filter, without the share group.
The body takes the started app as an argument, so the live test below runs it too.

## A cold freezer

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust title="src/main.rs" reference="examples/guide/testing.rs#cold"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust title="src/main.rs" reference="examples/manual/guide/testing.rs#cold"
```

</TabItem>
</Tabs>

`settled` checks the outcome the handler returned.
[`tb.out::<Alerts>()`](api:ruststream::testing::TestApp::out) reads what the handler published through its `Alerts` slot, and `assert_not_called` checks that it published nothing.

## A delay, without waiting for it

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust title="src/main.rs" reference="examples/guide/testing.rs#delayed"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust title="src/main.rs" reference="examples/manual/guide/testing.rs#delayed"
```

</TabItem>
</Tabs>

The test starts on a paused clock, and [`advance`](api:ruststream::testing::TestApp::advance) moves it forward.
Ten seconds of `RETRY` cost nothing.
`retry_after` publishes a copy of the reading to `freezers/retry/temperature` when the delay ends, and the filter reads it again.
The copy carries the retry count in a header, and `published` shows it.
The third delivery is the last one the mount site allows, so the reading goes to `freezers/review` as it arrived.
Each delayed delivery is recorded as `Outcome::Nack`.

## What the in-process server models

The in-process server reads the broker's own settings, and a publish goes through the same validation and the same mapping onto MQTT 5 properties as on the wire.
It models the server one connection talks to:

- A publish reaches every subscription whose filter matches its topic, with `+`, `#` and the rule for topics that start with `$`.
- A share group takes one copy between its members, and overlapping filters each receive their own.
- A retained message reaches a later subscription outside a share group, and an empty retained payload clears it.
- A message larger than `max_packet_size` never arrives, and a topic or header the client refuses fails the publish.
- An acknowledgement after shutdown is refused.

Each broker connected in process is a server of its own.
A persistent session and its resumption, the last will, the receive-maximum window, the `QoS` 1 and 2 handshakes and several connections on one server need a real broker.

## The same test on Mosquitto

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust title="src/main.rs" reference="examples/guide/testing.rs#live"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust title="src/main.rs" reference="examples/manual/guide/testing.rs#live"
```

</TabItem>
</Tabs>

[`TestApp::start_live`](../components/testing/live.md) connects the same app to a running server.
The test reads the address from `MQTT_TEST_URL` with `live::url`, a helper of the example's own, and returns at once when it is unset.
It connects with a client id of its own, so it shares the server with other runs.
The assertions are the in-process ones, unchanged.

```bash
cargo test
MQTT_TEST_URL=mqtt://localhost:1883 cargo test -- --test-threads=1
```

`RUSTSTREAM_REQUIRE_LIVE=1` turns the skip into a failure, for a CI job that starts a server and must not pass without it.
