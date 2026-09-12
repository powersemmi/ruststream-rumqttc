# ruststream-rumqttc { #ruststream-rumqttc }

**`ruststream-rumqttc`** 让 [RustStream](https://powersemmi.github.io/ruststream/) 服务订阅 MQTT 5
主题，并向主题发布消息，底层走 [`rumqttc`](https://docs.rs/rumqttc)。消息头以 MQTT 5 用户属性发送，
因此非 Rust 的对端看到的是普通的 MQTT 消息。

你可以订阅带通配符的主题过滤器、选择服务质量、发布保留消息，以及配置会话和遗嘱消息。服务质量和
保留标志在发布者上声明一次；某一条消息需要不同取值时，在这一次发布上改。共享订阅把一个主题的消息
分给互相竞争的消费者。打开 `testing` feature，你可以让服务的处理器跑在进程内 Broker 上，不需要
服务器。

```toml
ruststream = { version = "0.7", features = ["macros", "json"] }
ruststream-rumqttc = "0.7"
serde = { version = "1", features = ["derive"] }
```

应用函数指定 Broker 并挂载处理器：

```rust
--8<-- "crates/ruststream-rumqttc/examples/mqtt_service.rs:app"
```

## 下一步去哪 { #where-to-go-next }

<div class="grid cards" markdown>

- :material-access-point: **[MQTT 指南](mqtt.md)** - 主题过滤器、服务质量、共享订阅、保留发布和测试。
- :material-book-open-variant: **[RustStream 文档](https://powersemmi.github.io/ruststream/)** - 框架本身：订阅者、路由、编解码器、中间件、CLI。
- :material-language-rust: **[API 参考](https://docs.rs/ruststream-rumqttc)** - 这个 crate 在 docs.rs 上的 rustdoc。

</div>

## 本站与 RustStream 文档的关系 { #how-this-site-relates-to-the-ruststream-docs }

本站讲的是 MQTT Broker。在每个 Broker 上表现一致的框架概念，写在
[RustStream 文档](https://powersemmi.github.io/ruststream/)里。
