# ruststream-rumqttc

**`ruststream-rumqttc`** is the MQTT 5 broker for the
[RustStream](https://powersemmi.github.io/ruststream/) messaging framework, built on
[`rumqttc`](https://docs.rs/rumqttc). It covers topic filters with wildcards, quality of service,
shared subscriptions, retained messages, sessions and wills, and ships an in-process test broker
under its `testing` feature.

Handlers, routers, codecs, and middleware come from the framework; this crate supplies the
transport, and nothing broker-specific leaks back into the framework.

MQTT 5 is the target version because two things the framework relies on exist only there: user
properties, which carry headers natively instead of through an invented envelope, and shared
subscriptions, which make competing consumers expressible.

```toml
ruststream = { version = "0.6", features = ["macros", "json"] }
ruststream-rumqttc = "0.6"
serde = { version = "1", features = ["derive"] }
```

```rust
--8<-- "crates/ruststream-rumqttc/examples/mqtt_service.rs:app"
```

## Where to go next

<div class="grid cards" markdown>

- :material-access-point: **[MQTT guide](mqtt.md)** - topic filters, quality of service, shared subscriptions, retained publishes, and testing.
- :material-book-open-variant: **[RustStream docs](https://powersemmi.github.io/ruststream/)** - the framework itself: subscribers, routing, codecs, middleware, the CLI.
- :material-language-rust: **[API reference](https://docs.rs/ruststream-rumqttc)** - the crate's rustdoc on docs.rs.

</div>

## How this site relates to the RustStream docs

This site documents the MQTT broker only. Framework concepts that apply to every broker (writing
subscribers, publishing, routing, codecs, middleware, observability, the CLI) live in the
[RustStream documentation](https://powersemmi.github.io/ruststream/). The pages here cover what is
specific to MQTT and link back to the framework docs where the two meet.
