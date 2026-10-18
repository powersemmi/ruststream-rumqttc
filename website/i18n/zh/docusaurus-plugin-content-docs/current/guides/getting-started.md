---
title: 快速开始
sidebar_position: 1
description: "安装 ruststream-rumqttc，启动本地 Mosquitto，运行第一个订阅者，并在没有服务器的情况下测试它。"
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# 快速开始

本指南安装 `ruststream-rumqttc`，启动一个本地 MQTT 服务器，并让第一个服务连上它运行。

## 安装 {/* #installation */}

```toml title="Cargo.toml"
[dependencies]
ruststream = { version = "0.7", features = ["macros", "json", "logging"] }
ruststream-rumqttc = "0.7"
serde = { version = "1", features = ["derive"] }
tracing = "0.1"

[dev-dependencies]
ruststream-rumqttc = { version = "0.7", features = ["testing"] }
tokio = { version = "1", features = ["macros", "rt"] }
```

`ruststream` 是框架本身：`macros` 提供属性宏，`json` 提供编解码器，`logging` 把服务的日志打印到终端。
`ruststream-rumqttc` 是 MQTT 5 Broker。
Broker 自己不启用任何编解码器，所以服务在 `ruststream` 上选择 `json`、`msgpack` 或 `cbor`。

这个 Broker crate 有两个 feature，默认都不启用：

| Feature | 提供的内容 |
|---|---|
| `testing` | `TestApp` 背后的[进程内 MQTT 服务器](../components/testing/index.md) |
| `asyncapi` | 生成的 AsyncAPI 文档中的 [MQTT 绑定](../components/asyncapi/index.md) |

## 系统要求 {/* #system-requirements */}

`rumqttc` 客户端通过 rustls 使用 TLS，其密码学实现来自 `aws-lc-rs`。
`aws-lc-rs` 要编译 C 代码，所以构建需要 C 编译器。
在 Debian 和 Ubuntu 上，`build-essential` 提供它。
其他平台见 [aws-lc-rs 的构建要求](https://aws.github.io/aws-lc-rs/requirements/index.html)。

## 本地 MQTT 服务器 {/* #a-local-mqtt-server */}

开发时，一个 Eclipse Mosquitto 容器就够了。
它从配置文件读取监听端口：

```text title="mosquitto.conf"
listener 1883
allow_anonymous true
```

```yaml title="compose.yaml"
services:
  mosquitto:
    image: eclipse-mosquitto:2
    ports:
      - "1883:1883"
    volumes:
      - ./mosquitto.conf:/mosquitto/config/mosquitto.conf:ro
```

```bash
docker compose up -d
```

服务器监听 `localhost:1883`，客户端无需密码即可连接。

## 第一个服务 {/* #the-first-service */}

<Tabs groupId="api">
<TabItem value="macros" label="宏">

```rust title="src/main.rs" reference="examples/guide/getting_started.rs#service"
```

</TabItem>
<TabItem value="manual" label="手写">

```rust title="src/main.rs" reference="examples/manual/guide/getting_started.rs#service"
```

</TabItem>
</Tabs>

`record` 读取每台设备发来的 JSON 遥测数据。
它对一条读数执行 ack，低于绝对零度的读数则被丢弃。
[`MqttFilter`](../components/subscriptions/mqtt-filter.md) 订阅一个主题过滤器：`+` 代表主题中的一个层级，这里是设备的 id。
[`Qos::AtLeastOnce`](../components/subscriptions/qos.md) 让每条读数都成为需要确认的投递：`record` 返回时，确认才发出。
过滤器匹配许多主题，所以挂载点要指定[延迟重试](../components/settlement/out-retry.md)发往的主题；每个基于过滤器的订阅都要指定一个，否则服务拒绝启动。

[`MqttBroker::new`](../components/broker/index.md) 接收服务器的 URL 和客户端 ID，只把它们记录下来。
连接在服务启动时才打开。
客户端 ID 是服务在服务器上的会话的名字，所以每个运行中的进程都有自己的 ID。
`#[ruststream::app]` 把 `app` 变成 `main`；在手写形式中，[`run_main`](api:ruststream::runtime::cli::run_main) 做同样的事。

运行服务：

```bash
cargo run -- run
```

在另一个终端里，用镜像自带的客户端发布一条读数：

```bash
docker compose exec mosquitto mosquitto_pub -V mqttv5 -q 1 \
  -t devices/dev42/telemetry -m '{"device":"dev42","temperature":21.5}'
```

服务记录一条 `reading recorded` 日志，带有设备和它的温度。
<kbd>Ctrl</kbd>+<kbd>C</kbd> 停止服务。

## 第一个测试 {/* #the-first-test */}

示例文件里还有两个测试。
每个测试都用 `TestApp::start` 启动同一个 `app`，向 `devices/dev42/telemetry` 发布一条读数，然后检查 `record` 如何结算它。
在测试中，`MqttBroker` 连接的是进程内 MQTT 服务器，所以测试不需要 Mosquitto：

```bash
cargo test
```

[测试 MQTT 服务](testing.md)逐一讲解这些测试，以及进程内服务器模拟了什么。

## 从 rumqttc 迁移 {/* #coming-from-rumqttc */}

基于 rumqttc 的服务要自己驱动事件循环，并手动确认每条消息。
下表列出每一部分在 RustStream 中的位置：

| rumqttc | RustStream |
|---|---|
| `MqttOptions` 及其 setter | [`MqttBroker`](../components/broker/index.md) 的构建器：[`keep_alive`](../components/broker/limits.md)、[`credentials`](../components/broker/security.md)、[`clean_start`](../components/broker/session.md)、[`last_will`](../components/broker/last-will.md) |
| `EventLoop::poll` 循环 | Broker 自己的连接任务，它负责重连和重新订阅 |
| `AsyncClient::subscribe` | [`MqttTopic`](../components/subscriptions/mqtt-topic.md) 或 [`MqttFilter`](../components/subscriptions/mqtt-filter.md) 上的 `#[subscriber]` 处理器 |
| 反序列化 `publish.payload` | 处理器的参数类型，由编解码器解码 |
| `set_manual_acks(true)` 和 `AsyncClient::ack` | 处理器的结果（[投递保证](delivery.md)） |
| `AsyncClient::publish` | 回复或 `Out` 参数，经由[发布策略](../components/publishing/index.md)发送（[发布命令](publishing.md)） |
