<h1 align="center">ruststream-rumqttc</h1>

<p align="center">
  <i>The MQTT 5 broker for the <a href="https://github.com/powersemmi/ruststream">RustStream</a> messaging framework: device and edge messaging with native headers, shared subscriptions, and QoS-aware acknowledgement.</i>
</p>

<p align="center">
  <a href="https://github.com/powersemmi/ruststream-rumqttc/actions/workflows/ci.yml"><img src="https://github.com/powersemmi/ruststream-rumqttc/actions/workflows/ci.yml/badge.svg" alt="CI"></a>
  <img src="https://img.shields.io/badge/MSRV-1.85-blue.svg" alt="MSRV 1.85">
  <img src="https://img.shields.io/badge/license-Apache--2.0-blue.svg" alt="License">
  <a href="https://t.me/ruststream_community"><img src="https://img.shields.io/badge/-Telegram-blue?logo=telegram&label=News" alt="Telegram news channel"></a>
  <a href="https://t.me/ruststream_communuty_ru_chat"><img src="https://img.shields.io/badge/-Telegram-blue?logo=telegram&label=RU" alt="Telegram RU chat"></a>
</p>

---

`ruststream-rumqttc` implements the RustStream broker contract over [`rumqttc`](https://crates.io/crates/rumqttc). Handlers, routers, codecs, and middleware come from the framework; this crate supplies the transport - and nothing broker-specific leaks back into the framework.

MQTT 5 is the primary target because two things the framework relies on exist only there: user properties (headers travel natively, without a wrapper envelope) and shared subscriptions (which make competing consumers expressible).

## Features

- **A crate-owned connection task.** The client exposes a single event loop that must be polled continuously; the crate drives it in a dedicated task that demultiplexes packets into independent per-subscription streams by topic-filter matching, reconnects with exponential backoff (the client itself retries with zero delay, forever), and resubscribes exactly when the broker reports the session gone, without stalling keep-alive traffic. Delivery back-pressure is the protocol's receive-maximum, which bounds unacknowledged deliveries.
- **QoS-aware acknowledgement.** QoS 1/2 acknowledge through the protocol under manual control (the client completes the QoS 2 handshake); QoS 0 has no protocol acknowledgement, so it reports `AckError::Unsupported` instead of reporting success. MQTT has no negative acknowledgement, so `nack(requeue = true)` reports `Unsupported` too - unacked messages redeliver when a persistent session resumes - and `nack(requeue = false)` acknowledges.
- **Shared subscriptions.** `MqttTopic::new("jobs").shared("workers")` subscribes `$share/workers/jobs`; the broker splits the stream across the group, and two group members on one connection round-robin locally (they are one wire subscription).
- **Wildcards as the protocol defines them** (`+`, `#`), with messages reporting the real topic they arrived on.
- **Headers ride user properties**; the well-known `content-type`, `reply-to`, and `correlation-id` headers ride the matching first-class MQTT 5 properties.
- **Sessions, wills, retained.** `clean_start`/`session_expiry` for persistent sessions, `last_will` on the broker, `retain` on the publish policy, TLS with client certificates (`tls_ca` + `tls_client_auth`) for managed MQTT services.
- **In-process test broker** (feature `testing`). `MqttTestBroker` reproduces core routing with no server, implements `ruststream::testing::TestableBroker`, and passes the framework's conformance suite in process.

## Status

Implemented and verified against Eclipse Mosquitto 2 (the framework's conformance lifecycle suite and the integration tests, including shared subscriptions and wildcard demultiplexing, run in CI against it). Tracks the `ruststream` 0.6 line; the crate itself is not published to crates.io yet. Design and scope are tracked in [powersemmi/ruststream#191](https://github.com/powersemmi/ruststream/issues/191).

## Write a service

```rust
use std::time::Duration;

use ruststream::runtime::{App, AppInfo, HandlerResult, RustStream};
use ruststream::subscriber;
use ruststream_rumqttc::{MqttBroker, MqttTopic, Qos};
use serde::Deserialize;

#[derive(Debug, Deserialize)]
struct Telemetry {
    temperature: f64,
}

#[subscriber(MqttTopic::new("devices/+/telemetry").qos(Qos::AtLeastOnce).shared("workers"))]
async fn handle(telemetry: &Telemetry) -> HandlerResult {
    println!("temperature: {}", telemetry.temperature);
    HandlerResult::Ack
}

#[ruststream::app]
fn app() -> impl App {
    RustStream::new(AppInfo::new("telemetry", "0.1.0")).with_broker(
        MqttBroker::new("mqtt://localhost:1883", "telemetry-svc")
            .keep_alive(Duration::from_secs(30))
            .clean_start(false)
            .session_expiry(Duration::from_secs(3600)),
        |b| {
            b.include(handle);
        },
    )
}
```

## Test it

The `testing` feature runs handlers against an in-process MQTT stand-in - no server, same routing. Protocol behaviour (QoS handshakes, shared groups, session redelivery, retained messages) is covered by the env-gated live suite instead: `just test-brokers` starts mosquitto and runs the integration tests plus the framework conformance lifecycle against it.

## Layout

```
ruststream-rumqttc/
├── crates/
│   └── ruststream-rumqttc/     the published crate
│       └── examples/           runnable mqtt_* examples
├── docker-compose.test.yml     mosquitto for the live suite
└── Cargo.toml                  workspace
```

## Contributing

```bash
just check          # fmt, clippy, feature checks
just test           # handler-stub tests, no server
just test-brokers   # live integration + conformance against mosquitto
```

## License

Licensed under the [Apache-2.0](./LICENSE) license.
