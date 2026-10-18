---
sidebar_position: 1
title: Live
description: "TestApp::start_live runs the same test body against a running MQTT server, for what only a server has: sessions, the last will, flow control."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Live

[`TestApp::start_live`](api:ruststream::testing::TestApp::start_live) runs the same test body against a running MQTT server, for what only a server has: sessions, the last will, flow control.

It connects the broker through its ordinary connect, so startup waits for the server's `CONNACK` and each subscription for its `SUBACK`.
The body is the one an in-process test runs; only the start call differs.
A publish, `settle` and `advance` wait on the real clock until every subscription has handled what was published to it.
The wait lasts ten seconds at most, and [`start_live_within`](api:ruststream::testing::TestApp::start_live_within) sets another bound.

The live mode is for what the in-process server leaves out: the login and TLS, a [persistent session](../broker/session.md), the [last will](../broker/last-will.md), the [receive window](../broker/limits.md), a [lost connection](../broker/reconnect.md), and several connections sharing one server.
A server is shared with whatever else runs against it.
So each run connects under a client id of its own: a second connection under one id takes the session over.

In the example, `live::url` returns the server's address from `MQTT_TEST_URL`, and the test returns early without one, so `cargo test` passes on a machine with no server.
A job that stood a server up sets `RUSTSTREAM_REQUIRE_LIVE=1`, and a missing address then fails the test instead of skipping it.
The repository's stand is a Mosquitto container in `docker-compose.test.yml`, which `just brokers-up` starts, with a listener that asks for a password and one that speaks TLS.

## Example

The two tests share one body: one in process, one against the server at `MQTT_TEST_URL`.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/mqtt_testing_live.rs#both"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/mqtt_testing_live.rs#both"
```

</TabItem>
</Tabs>
