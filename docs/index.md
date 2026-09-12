# ruststream-rumqttc

**`ruststream-rumqttc`** subscribes a [RustStream](https://powersemmi.github.io/ruststream/)
service to MQTT 5 topics and publishes to them, over [`rumqttc`](https://docs.rs/rumqttc). Headers
are sent as MQTT 5 user properties, so non-Rust peers see plain MQTT messages.

You can subscribe to topic filters with wildcards, choose the quality of service, publish retained
messages, and set up sessions and last wills. A shared subscription splits a topic's messages
between competing consumers. With the `testing` feature you can run a service's handlers against an
in-process broker, with no server.

```toml
ruststream = { version = "0.7", features = ["macros", "json"] }
ruststream-rumqttc = "0.7"
serde = { version = "1", features = ["derive"] }
```

The app function names the broker and includes the handler:

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

This site documents the MQTT broker. Framework concepts that work the same on every broker live in
the [RustStream documentation](https://powersemmi.github.io/ruststream/).
