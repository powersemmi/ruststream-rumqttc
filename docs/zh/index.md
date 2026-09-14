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

这个 crate 的指南就是它的 rustdoc。docs.rs 上的 [crate 概览](https://docs.rs/ruststream-rumqttc)
讲了[订阅][subscribing]，包括两个描述符、通配符、共享组和批；[确认][ack]，以及在没有服务端重投的
传输上每个处理器结果的含义；[发布][publishing]，包括单条消息的服务质量和保留标志；
[生成的文档][asyncapi]；在进程内 Broker 上做的[测试][testing]；还有[连接设置][operations]。

<div class="grid cards" markdown>

- :material-access-point: **[crate 概览](https://docs.rs/ruststream-rumqttc)** - MQTT 指南和 API 参考，在 docs.rs 上合成一份文档。
- :material-book-open-variant: **[RustStream 文档](https://powersemmi.github.io/ruststream/)** - 安装、快速上手、教程和 Broker 列表。
- :material-language-rust: **[框架参考](https://docs.rs/ruststream/latest/ruststream/)** - 订阅者、路由、编解码器、中间件、CLI。

</div>

## 本站与 RustStream 文档的关系 { #how-this-site-relates-to-the-ruststream-docs }

本站是 MQTT Broker 的入口页，它从前讲的内容现在都在 docs.rs 的 crate 概览里。在每个 Broker 上
表现一致的框架概念，写在 [RustStream 文档](https://powersemmi.github.io/ruststream/)里。

[subscribing]: https://docs.rs/ruststream-rumqttc/latest/ruststream_rumqttc/index.html#subscribing
[ack]: https://docs.rs/ruststream-rumqttc/latest/ruststream_rumqttc/index.html#acknowledgement
[publishing]: https://docs.rs/ruststream-rumqttc/latest/ruststream_rumqttc/index.html#publishing
[asyncapi]: https://docs.rs/ruststream-rumqttc/latest/ruststream_rumqttc/index.html#the-generated-document
[testing]: https://docs.rs/ruststream-rumqttc/latest/ruststream_rumqttc/index.html#testing
[operations]: https://docs.rs/ruststream-rumqttc/latest/ruststream_rumqttc/index.html#operations
