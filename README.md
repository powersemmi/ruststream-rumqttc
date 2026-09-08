<h1 align="center">ruststream-rumqttc</h1>

<p align="center">
  <i>The MQTT 5 broker for the <a href="https://github.com/powersemmi/ruststream">RustStream</a> messaging framework: device and edge messaging with native headers, shared subscriptions, and QoS-aware acknowledgement.</i>
</p>

<p align="center">
  <a href="https://github.com/powersemmi/ruststream-rumqttc/actions/workflows/ci.yml"><img src="https://github.com/powersemmi/ruststream-rumqttc/actions/workflows/ci.yml/badge.svg" alt="CI"></a>
  <a href="https://crates.io/crates/ruststream-rumqttc"><img src="https://img.shields.io/crates/v/ruststream-rumqttc.svg" alt="crates.io"></a>
  <a href="https://crates.io/crates/ruststream-rumqttc"><img src="https://img.shields.io/crates/dr/ruststream-rumqttc" alt="Recent downloads"></a>
  <a href="https://docs.rs/ruststream-rumqttc"><img src="https://img.shields.io/docsrs/ruststream-rumqttc" alt="docs.rs"></a>
  <img src="https://img.shields.io/badge/MSRV-1.88-blue.svg" alt="MSRV 1.88">
  <img src="https://img.shields.io/badge/license-Apache--2.0-blue.svg" alt="License">
  <a href="https://t.me/ruststream_community"><img src="https://img.shields.io/badge/-Telegram-blue?logo=telegram&label=News" alt="Telegram news channel"></a>
  <a href="https://t.me/ruststream_communuty_ru_chat"><img src="https://img.shields.io/badge/-Telegram-blue?logo=telegram&label=RU" alt="Telegram RU chat"></a>
</p>

<p align="center">
  <b><a href="https://powersemmi.github.io/ruststream-rumqttc/">Documentation</a></b>
</p>

---

`ruststream-rumqttc` implements the RustStream broker contract over [`rumqttc`](https://crates.io/crates/rumqttc). Handlers, routers, codecs, and middleware come from the framework; this crate supplies the transport - and nothing broker-specific leaks back into the framework.

MQTT 5 is the primary target because two things the framework relies on exist only there: user properties (headers travel natively, without a wrapper envelope) and shared subscriptions (which make competing consumers expressible).

## Features

- **A crate-owned connection task.** The client exposes a single event loop that must be polled continuously; the crate drives it in a dedicated task that demultiplexes packets into independent per-subscription streams by topic-filter matching, reconnects with exponential backoff (the client itself retries with zero delay, forever), and resubscribes exactly when the broker reports the session gone, without stalling keep-alive traffic. Delivery back-pressure is the protocol's receive-maximum, which bounds unacknowledged deliveries.
- **QoS-aware acknowledgement.** QoS 1/2 acknowledge through the protocol under manual control (the client completes the QoS 2 handshake); QoS 0 has no protocol acknowledgement, so it reports `AckError::Unsupported` instead of reporting success. MQTT has no negative acknowledgement, so `nack(requeue = true)` reports `Unsupported` too - unacked messages redeliver when a persistent session resumes - and `nack(requeue = false)` acknowledges.
- **Shared subscriptions.** `MqttTopic::new("jobs").shared("workers")` subscribes `$share/workers/jobs`; the broker splits the stream across the group, and two group members on one connection round-robin locally (they are one wire subscription).
- **Wildcards as the protocol defines them** (`+`, `#`), with messages reporting the real topic they arrived on.
- **Batches for `&[T]` handlers.** A PUBLISH packet carries one message, so the crate assembles batches on the client to the size the mount site named (`b.include(ingest.batch(nonzero!(64)))`), closing a partial batch 20 ms after its first delivery. Nothing at the mount site says which side of the wire filled it.
- **Headers ride user properties**; the well-known `content-type`, `reply-to`, and `correlation-id` headers ride the matching first-class MQTT 5 properties.
- **Payloads that are already bytes.** An MQTT payload is often a wire value the service holds already - a state string, a sensor frame, a protobuf record - rather than a model to encode. `#[derive(Serialized)]` on the way out and `#[derive(Deserialized)]` on the way in move those bytes untouched, with no codec on the path; the outgoing type still names its topic through `#[derive(Outgoing)]`, `{placeholder}` segments included.
- **Sessions, wills, retained.** `clean_start`/`session_expiry` for persistent sessions, `last_will` on the broker, `retain` on the publish policy, TLS with client certificates (`tls_ca` + `tls_client_auth`) for managed MQTT services.
- **Per-message QoS and retain.** `MqttPublishOptions` reopens the two arguments MQTT carries on every PUBLISH packet: `publisher.with_retain(true).message(&state).publish()`. The steps resolve on an `Out` slot entry too, so a handler that needs them binds `Out<impl MqttPublishOptions>` and its publish stays attributed to that slot.
- **One glob per routes file.** `ruststream_rumqttc::prelude::*` carries the framework's prelude plus this crate's surface, with `MqttPublish` aliased to `Publish`, so a mount site reads the same whichever broker it runs on. A handler body imports `ruststream::prelude::*` alone and states a capability on its injected publisher, so it names no broker type at all.
- **In-process test broker** (feature `testing`). `MqttTestBroker` reproduces the crate's core routing with no server; its connected form implements `ruststream::testing::TestableBroker`, so it drives the `TestApp` harness and passes the framework's conformance suite in process.

## Install

```toml
[dependencies]
ruststream = { version = "0.7", features = ["macros", "json"] }
ruststream-rumqttc = "0.7"
serde = { version = "1", features = ["derive"] }

[dev-dependencies]
ruststream-rumqttc = { version = "0.7", features = ["testing"] }
tokio = { version = "1", features = ["rt-multi-thread", "macros"] }
```

## Write a service

```rust
use std::time::Duration;

use ruststream_rumqttc::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Debug, Deserialize, Serialize)]
struct Telemetry {
    device: String,
    temperature: f64,
}

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
#[outgoing(name = "alerts")]
struct Alert {
    device: String,
}

#[subscriber(MqttTopic::new("devices/+/telemetry").qos(Qos::AtLeastOnce).shared("workers"))]
async fn handle(telemetry: &Telemetry, Out(alerts): Out<impl Publisher>) -> HandlerOutcome {
    if telemetry.temperature <= 30.0 {
        return HandlerOutcome::ack();
    }
    let alert = Alert {
        device: telemetry.device.clone(),
    };
    if alerts.message(&alert).publish().await.is_err() {
        return HandlerOutcome::retry();
    }
    HandlerOutcome::ack()
}

#[ruststream::app]
fn app() -> impl App {
    RustStream::new(AppInfo::new("telemetry", "0.1.0")).with_broker(
        MqttBroker::new("mqtt://localhost:1883", "telemetry-svc")
            .keep_alive(Duration::from_secs(30))
            .clean_start(false)
            .session_expiry(Duration::from_secs(3600)),
        |b| {
            b.include(handle)
                .out(DefaultSlot, Publish::default().qos(Qos::AtLeastOnce))
                .build();
        },
    )
}
```

`#[ruststream::app]` generates `main`, so there is no runtime boilerplate. `.out(marker, policy)` is the mount site's one publish verb: `DefaultSlot` names the handler's single unnamed `Out`, `Reply` the reply slot of a `publish(..)` handler, and a `#[derive(OutSlot)]` marker any further one. The policy carries the MQTT arguments, so the body states a capability (`Out<impl Publisher>`) and never a broker type - which is what lets the same handler run under the harness below.

## Test it

The `testing` feature ships an in-process transport: no server, the crate's own routing, the same lifecycle ladder. Mount the service on `MqttTestBroker` and drive it with the framework's `TestApp`, which runs the production dispatch path and settles the reaction before an assertion reads it. `MqttTopic` is the live broker's subscription descriptor, so the handler under test names its subject as a plain topic string - the form that mounts on either transport. `Telemetry` and `Alert` carry both serde derives because the test injects one and reads the other back.

```rust
use ruststream::testing::TestApp;
use ruststream_rumqttc::prelude::*;
use ruststream_rumqttc::testing::{MqttTestBroker, MqttTestPublish};

#[subscriber("devices/dev42/telemetry")]
async fn raise_alert(telemetry: &Telemetry, Out(alerts): Out<impl Publisher>) -> HandlerOutcome {
    // the body from the service above, unchanged
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_hot_reading_raises_an_alert() -> Result<(), Box<dyn std::error::Error>> {
    let app = RustStream::new(AppInfo::new("telemetry", "0.1.0")).with_broker(
        MqttTestBroker::new(),
        |b| {
            b.include(raise_alert)
                .out(DefaultSlot, MqttTestPublish)
                .build();
        },
    );
    let tb = TestApp::start(app).await?;

    tb.broker::<MqttTestBroker>()
        .publish(
            "devices/dev42/telemetry",
            &Telemetry {
                device: "dev42".to_owned(),
                temperature: 31.5,
            },
        )
        .await?;

    tb.broker::<MqttTestBroker>()
        .published::<Alert>("alerts")
        .assert_called_once()
        .with(&Alert {
            device: "dev42".to_owned(),
        });
    Ok(())
}
```

The compiling original lives in `crates/ruststream-rumqttc/tests/handlers_mqtt.rs`, next to the same harness driving the per-message arguments on a slot and a batch handler.

Protocol behaviour (QoS handshakes, shared groups, session redelivery, retained messages) is covered by the env-gated live suite instead: `just test-brokers` starts mosquitto and runs the integration tests plus the framework conformance lifecycle against it.

## Layout

```
ruststream-rumqttc/
├── crates/
│   └── ruststream-rumqttc/     the published crate
│       ├── examples/           runnable mqtt_* examples
│       └── tests/              handler tests, the live suite, conformance
├── docs/                       the documentation site
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
