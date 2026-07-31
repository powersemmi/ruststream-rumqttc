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

`ruststream-rumqttc` will implement the [RustStream](https://github.com/powersemmi/ruststream) broker contract over [`rumqttc`](https://crates.io/crates/rumqttc) (MQTT 3.1.1 and 5). Handlers, routers, codecs, and middleware come from the framework; this crate supplies the transport - and nothing broker-specific leaks back into the framework.

## Status

**Not implemented yet.** This repository is a scaffold: the workspace, CI, and release plumbing are in place, and the crate is an empty stub. The implementation will target the `ruststream` 0.6 line; the design and scope are tracked in [powersemmi/ruststream#191](https://github.com/powersemmi/ruststream/issues/191).

## Planned surface

- MQTT 5 as the primary target: user properties carry headers natively, and shared subscriptions express competing consumers.
- A crate-owned event-loop task that demultiplexes packets into independent per-subscription streams without stalling keep-alive traffic.
- QoS-aware acknowledgement: manual acks for at-least-once and exactly-once, `AckError::Unsupported` for at-most-once.
- TLS with client certificates as part of the first release; retained messages, last will, and session persistence as configuration.
- MQTT 3.1.1 as a documented compatibility mode with its limitations stated.

The broker contract (lazy startup, the typed connect/shutdown lifecycle, and the optional capability traits) is defined by [`ruststream`](https://crates.io/crates/ruststream) and verified by `ruststream::conformance`, with the suite run against a real broker before release.

## Contributing

```bash
just check   # fmt, clippy, feature checks
just test    # tests
just ci      # the full local gate
```

## License

Licensed under the [Apache-2.0](./LICENSE) license.
