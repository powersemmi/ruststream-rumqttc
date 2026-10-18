---
title: 测试 MQTT 服务
sidebar_position: 8
description: "用 TestApp 在进程内测试 MQTT 服务：生产环境的应用、通配符主题、发布的告警、暂停时钟上的延迟，以及在 Mosquitto 上运行的同一个测试。"
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# 测试 MQTT 服务

测试用 [`TestApp`](api:ruststream::testing::TestApp) 运行服务自己的应用，连接的是[进程内 MQTT 服务器](../components/testing/index.md)或者 Mosquitto。

被测服务负责监控冷柜。
每台冷柜把温度发布到 `freezers/<id>/temperature`，各个监控实例在共享组中分摊这些读数。
每条读数都写入温度日志。
温度高于 -18 °C 的冷柜会在 `freezers/alerts` 上触发告警。
日志没有应答时，处理器用 [`retry_after`](../components/settlement/retry-after.md) 要求十秒后再投递这条读数。
经过三次投递后，读数进入[死信主题](../components/settlement/dead-letters.md) `freezers/review`。

处理器 `watch` 经由 `Alerts` 槽位发布告警。
应用由 `service(broker)` 构建，所以测试可以选择 Broker，而 `main` 使用生产环境的 Broker。
测试需要在 `[dev-dependencies]` 中启用 `ruststream-rumqttc` 的 `testing` feature。

## 过热的冷柜 {/* #a-warm-freezer */}

<Tabs groupId="api">
<TabItem value="macros" label="宏">

```rust title="src/main.rs" reference="examples/guide/testing.rs#in_process"
```

</TabItem>
<TabItem value="manual" label="手写">

```rust title="src/main.rs" reference="examples/manual/guide/testing.rs#in_process"
```

</TabItem>
</Tabs>

[`TestApp::start`](api:ruststream::testing::TestApp::start) 把 [`MqttBroker`](../components/broker/index.md) 连接到进程内 MQTT 服务器。
测试从不连接那个 URL。
测试按生产类型访问 Broker：`tb.broker::<MqttBroker>()`。
`Reading` 没有指定主题，所以测试像冷柜那样用 `.to("freezers/f1/temperature")` 指定一个。
处理器的过滤器像在 Mosquitto 上那样匹配它。
`publish` 在处理器及其告警都完成结算后才返回。
所以接下来的几行断言的是已经完成的状态：订阅如何结算，以及什么到达了 `freezers/alerts`。
测试按过滤器访问订阅，不带共享组。
测试主体把已启动的应用作为参数接收，所以下面的线上测试也运行它。

## 正常的冷柜 {/* #a-cold-freezer */}

<Tabs groupId="api">
<TabItem value="macros" label="宏">

```rust title="src/main.rs" reference="examples/guide/testing.rs#cold"
```

</TabItem>
<TabItem value="manual" label="手写">

```rust title="src/main.rs" reference="examples/manual/guide/testing.rs#cold"
```

</TabItem>
</Tabs>

`settled` 检查处理器返回的结果。
[`tb.out::<Alerts>()`](api:ruststream::testing::TestApp::out) 读取处理器经由 `Alerts` 槽位发布的内容，`assert_not_called` 检查它什么也没有发布。

## 不必等待的延迟 {/* #a-delay-without-waiting-for-it */}

<Tabs groupId="api">
<TabItem value="macros" label="宏">

```rust title="src/main.rs" reference="examples/guide/testing.rs#delayed"
```

</TabItem>
<TabItem value="manual" label="手写">

```rust title="src/main.rs" reference="examples/manual/guide/testing.rs#delayed"
```

</TabItem>
</Tabs>

测试在暂停的时钟上启动，[`advance`](api:ruststream::testing::TestApp::advance) 让时钟前进。
十秒的 `RETRY` 不花费任何时间。
延迟结束时，`retry_after` 把读数的副本发布到 `freezers/retry/temperature`，过滤器再次读取它。
副本在消息头中带着重试次数，`published` 把它显示出来。
第三次投递是挂载点允许的最后一次，所以这条读数按到达时的样子进入 `freezers/review`。
每次被延迟的投递都记录为 `Outcome::Nack`。

## 进程内服务器模拟了什么 {/* #what-the-in-process-server-models */}

进程内服务器读取 Broker 自己的设置，发布经过的校验以及到 MQTT 5 属性的映射都与线上相同。
它模拟的是一个连接所面对的服务器：

- 发布到达过滤器与其主题匹配的每个订阅，包括 `+`、`#` 以及以 `$` 开头的主题的规则。
- 共享组的成员之间只取一份副本，相互重叠的过滤器各自收到自己的一份。
- 保留消息会到达共享组之外的后来订阅，空的保留载荷会清除它。
- 大于 `max_packet_size` 的消息永远不会到达，客户端拒绝的主题或消息头会让发布失败。
- 关闭之后的确认会被拒绝。

在进程内连接的每个 Broker 都有一个自己的服务器。
持久会话及其恢复、遗嘱消息、Receive Maximum 窗口、`QoS` 1 和 2 的握手，以及同一个服务器上的多个连接，都需要真实的 Broker。

## 在 Mosquitto 上运行同一个测试 {/* #the-same-test-on-mosquitto */}

<Tabs groupId="api">
<TabItem value="macros" label="宏">

```rust title="src/main.rs" reference="examples/guide/testing.rs#live"
```

</TabItem>
<TabItem value="manual" label="手写">

```rust title="src/main.rs" reference="examples/manual/guide/testing.rs#live"
```

</TabItem>
</Tabs>

[`TestApp::start_live`](../components/testing/live.md) 把同一个应用连接到正在运行的服务器。
测试用 `live::url`（示例自带的辅助函数）从 `MQTT_TEST_URL` 读取地址，未设置时立即返回。
它用自己的客户端 ID 连接，所以可以和其他运行共用服务器。
断言与进程内的完全相同。

```bash
cargo test
MQTT_TEST_URL=mqtt://localhost:1883 cargo test -- --test-threads=1
```

`RUSTSTREAM_REQUIRE_LIVE=1` 把跳过变成失败，用于会启动服务器、没有服务器就不能通过的 CI 任务。
