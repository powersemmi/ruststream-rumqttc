<h1 align="center">ruststream-rumqttc</h1>

<p align="center">
  <i>The MQTT 5 broker for the <a href="https://github.com/powersemmi/ruststream">RustStream</a> messaging framework: device and edge messaging with native headers, shared subscriptions, and QoS-aware acknowledgement.</i>
</p>

<p align="center">
  <a href="https://github.com/powersemmi/ruststream-rumqttc/actions/workflows/ci.yml"><img src="https://github.com/powersemmi/ruststream-rumqttc/actions/workflows/ci.yml/badge.svg" alt="CI"></a>
  <a href="https://crates.io/crates/ruststream-rumqttc"><img src="https://img.shields.io/crates/v/ruststream-rumqttc.svg" alt="crates.io"></a>
  <a href="https://crates.io/crates/ruststream-rumqttc"><img src="https://img.shields.io/crates/dr/ruststream-rumqttc" alt="Recent downloads"></a>
  <a href="https://docs.rs/ruststream-rumqttc"><img src="https://img.shields.io/docsrs/ruststream-rumqttc" alt="docs.rs"></a>
  <img src="https://img.shields.io/badge/MSRV-1.95-blue.svg" alt="MSRV 1.95">
  <img src="https://img.shields.io/badge/license-Apache--2.0-blue.svg" alt="License">
  <a href="https://t.me/ruststream_community"><img src="https://img.shields.io/badge/-Telegram-blue?logo=telegram&label=News" alt="Telegram news channel"></a>
  <a href="https://t.me/ruststream_communuty_ru_chat"><img src="https://img.shields.io/badge/-Telegram-blue?logo=telegram&label=RU" alt="Telegram RU chat"></a>
</p>

<p align="center">
  <b><a href="https://powersemmi.github.io/ruststream-rumqttc/">Documentation</a></b>
</p>

---

`ruststream-rumqttc` connects a RustStream service to an MQTT 5 broker over
[`rumqttc`](https://crates.io/crates/rumqttc). Handlers, routing, codecs and middleware come from
the framework; this crate is the transport.

## Features

- **A connection task the crate owns:** one event loop per broker, per-subscription streams,
  reconnects with backoff and resubscribes when the session is gone.
- **QoS-aware acknowledgement** for QoS 1 and 2; QoS 0 reports that it has none.
- **Shared subscriptions** for competing consumers.
- **Overlapping filters on one connection,** each receiving a message once (MQTT 5 subscription
  identifiers); the same filter opened twice is one wire subscription.
- **Topics and filters:** `MqttTopic` for a topic, `MqttFilter` for the `+` and `#` wildcards.
- **Retry caps and dead-letter topics** declared where the handler is mounted.
- **Batches** assembled on the client.
- **Headers as user properties,** with `content-type`, `reply-to` and `correlation-id` as the
  native MQTT 5 properties.
- **Byte payloads without a codec:** `#[derive(Serialized)]` out, `#[derive(Deserialized)]` in.
- **Sessions, wills and retained messages,** QoS and retain per message
  (`publisher.message(&state).retain(true).publish()`), and TLS with client certificates.
- **AsyncAPI** with the specification's `mqtt` binding, behind the `asyncapi` feature.
- **Tests run the production app** (feature `testing`): `TestApp::start(app())` connects
  `MqttBroker` in process, with no server; `TestApp::start_live(app())` runs the same test
  against a real broker.

## Install

```toml
[dependencies]
ruststream = { version = "0.7", features = ["macros", "json"] }
ruststream-rumqttc = "0.7"
serde = { version = "1", features = ["derive"] }

[dev-dependencies]
ruststream-rumqttc = { version = "0.7", features = ["testing"] }
```

## Write a service

```rust
use std::time::Duration;

use ruststream_rumqttc::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Debug, Deserialize, Outgoing, Serialize)]
struct Telemetry {
    device: String,
    temperature: f64,
}

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
#[outgoing(name = "alerts")]
struct Alert {
    device: String,
}

#[subscriber(MqttFilter::new("devices/+/telemetry").qos(Qos::AtLeastOnce).shared("workers"))]
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
                .out_retry(Publish::default())
                .to("devices/retry/telemetry")
                .build();
        },
    )
}
```

`#[ruststream::app]` generates `main`, so the binary understands `run` and `asyncapi gen`.

## Test it

`TestApp` runs the service's own app with `MqttBroker` in process, with no server.
`TestApp::start_live(app())` runs the same test against a real broker.

```rust
use ruststream::testing::TestApp;

let tb = TestApp::start(app()).await?;

tb.broker::<MqttBroker>()
    .message(&Telemetry {
        device: "dev42".to_owned(),
        temperature: 31.5,
    })
    .to("devices/dev42/telemetry")
    .publish()
    .await?;

tb.broker::<MqttBroker>()
    .published::<Alert>("alerts")
    .assert_called_once()
    .with(&Alert {
        device: "dev42".to_owned(),
    });
tb.shutdown().await?;
```

## Documentation

- This crate: <https://docs.rs/ruststream-rumqttc>
- The framework: <https://powersemmi.github.io/ruststream/latest>

## Minimum supported Rust version

The MSRV is **1.95**, edition 2024.

## Contributing

See [CONTRIBUTING.md](./CONTRIBUTING.md).

```bash
just check          # fmt, clippy, feature checks
just test           # in-process tests, no server
just test-brokers   # live integration + conformance against mosquitto
```

## License

Licensed under the [Apache-2.0](./LICENSE) license.
