# ruststream-rumqttc

**`ruststream-rumqttc`** subscribes a [RustStream](https://powersemmi.github.io/ruststream/)
service to MQTT 5 topics and publishes to them, over [`rumqttc`](https://docs.rs/rumqttc). Headers
are sent as MQTT 5 user properties, so non-Rust peers see plain MQTT messages.

You can subscribe to topic filters with wildcards, choose the quality of service, publish retained
messages, and set up sessions and last wills. The quality of service and the retain flag are
declared once for a publisher, and changed on a single publish where one message needs to differ. A
shared subscription splits a topic's messages between competing consumers. With the `testing`
feature you can run a service's handlers against an in-process broker, with no server.

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

The crate's guide is its rustdoc. The [crate overview](https://docs.rs/ruststream-rumqttc) on
docs.rs covers [subscribing][subscribing] with the two descriptors, wildcards, share groups and
batches; [acknowledgement][ack] and what each handler outcome means on a transport with no
server-side retry; [publishing][publishing], including the quality of service and the retain flag
of a single message; [the generated document][asyncapi]; [testing][testing] against the in-process
broker; and [the connection settings][operations].

<div class="grid cards" markdown>

- :material-access-point: **[Crate overview](https://docs.rs/ruststream-rumqttc)** - the MQTT guide and the API reference, in one document on docs.rs.
- :material-book-open-variant: **[RustStream docs](https://powersemmi.github.io/ruststream/)** - installation, the quick start, the tutorial and the list of brokers.
- :material-language-rust: **[Framework reference](https://docs.rs/ruststream/latest/ruststream/)** - subscribers, routing, codecs, middleware, the CLI.

</div>

## How this site relates to the RustStream docs

This site is the entry page for the MQTT broker, and everything it used to explain now lives in the
crate overview on docs.rs. Framework concepts that work the same on every broker live in the
[RustStream documentation](https://powersemmi.github.io/ruststream/).

[subscribing]: https://docs.rs/ruststream-rumqttc/latest/ruststream_rumqttc/index.html#subscribing
[ack]: https://docs.rs/ruststream-rumqttc/latest/ruststream_rumqttc/index.html#acknowledgement
[publishing]: https://docs.rs/ruststream-rumqttc/latest/ruststream_rumqttc/index.html#publishing
[asyncapi]: https://docs.rs/ruststream-rumqttc/latest/ruststream_rumqttc/index.html#the-generated-document
[testing]: https://docs.rs/ruststream-rumqttc/latest/ruststream_rumqttc/index.html#testing
[operations]: https://docs.rs/ruststream-rumqttc/latest/ruststream_rumqttc/index.html#operations
