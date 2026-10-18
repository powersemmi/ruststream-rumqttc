---
title: 投递保证
sidebar_position: 3
description: "选择 MQTT 服务质量，了解处理器的每种结果发送什么，用持久会话跨越重启，并限制传输中的投递数量。"
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# 投递保证

本指南为每个订阅选择服务质量，说明处理器的每种结果向服务器发送什么，并保留服务停机期间错过的消息。

示例是一个计费服务。
电表上报用电量，每份报告都关系到钱，所以一份也不能丢。
电表还上报功率，供实时图表使用，丢掉的采样会被下一个取代。

## 服务质量 {/* #quality-of-service */}

MQTT 有三个[服务质量](../components/subscriptions/qos.md)等级：

| 等级 | 保证 | 报文 |
|---|---|---|
| `Qos::AtMostOnce` (0) | 至多一次：消息可能丢失 | `PUBLISH` |
| `Qos::AtLeastOnce` (1) | 至少一次：消息可能到达两次 | `PUBLISH`、`PUBACK` |
| `Qos::ExactlyOnce` (2) | 双方之间恰好一次 | `PUBLISH`、`PUBREC`、`PUBREL`、`PUBCOMP` |

订阅指定它读取时的等级，发布者指定它发送时的等级。
投递按两者中较低的一个进行。
因此，以 `QoS` 1 发布用电量的电表，到达 `QoS` 2 的订阅时仍是 `QoS` 1。

## 结算一份报告 {/* #settling-a-report */}

<Tabs groupId="api">
<TabItem value="macros" label="宏">

```rust title="src/main.rs" reference="examples/guide/delivery.rs#bill"
```

</TabItem>
<TabItem value="manual" label="手写">

```rust title="src/main.rs" reference="examples/manual/guide/delivery.rs#bill"
```

</TabItem>
</Tabs>

Broker 在处理器返回时确认投递，而不是在投递到达时。
每种[结果](../components/settlement/index.md)发送协议为它准备的内容：

- `ack` 在 `QoS` 1 下发送 `PUBACK`，在 `QoS` 2 下发送 `PUBREC`；`QoS` 2 握手的其余部分由客户端自己完成。
- `drop` 同样发送确认：MQTT 没有否定确认，确认是它了结一条消息的唯一方式。
- [`retry`](../components/settlement/retry.md) 什么也不发送。报告保持未确认状态，只有当服务的会话在新连接上恢复时，服务器才会再次发送它。

`QoS` 0 投递根本没有确认。
它的结果报告 `AckError::Unsupported`，运行时记录一条警告。

账本没有应答时，`bill` 返回 `retry`。
于是报告等待下一个会话：重启之后，服务器再次把它交过来。
[重试与死信主题](retries.md)展示如何在同一个会话内再试一次。

## QoS 0 下的批 {/* #batches-at-qos-0 */}

<Tabs groupId="api">
<TabItem value="macros" label="宏">

```rust title="src/main.rs" reference="examples/guide/delivery.rs#chart"
```

</TabItem>
<TabItem value="manual" label="手写">

```rust title="src/main.rs" reference="examples/manual/guide/delivery.rs#chart"
```

</TabItem>
</Tabs>

接收切片的处理器是[批处理器](../components/subscriptions/batches.md)。
一个 `PUBLISH` 报文只携带一条消息，所以 Broker 在客户端组装批。
批达到挂载点指定的大小，或者在第一个采样到达 20 毫秒后，就会关闭。
确认仍然按每次投递进行。

## 持久会话 {/* #a-persistent-session */}

<Tabs groupId="api">
<TabItem value="macros" label="宏">

```rust title="src/main.rs" reference="examples/guide/delivery.rs#app"
```

</TabItem>
<TabItem value="manual" label="手写">

```rust title="src/main.rs" reference="examples/manual/guide/delivery.rs#app"
```

</TabItem>
</Tabs>

[会话](../components/broker/session.md)是服务器为一个客户端 ID 保存的内容：它的订阅，以及它尚未确认的 `QoS` 1 和 2 消息。
[`clean_start(false)`](api:ruststream_rumqttc::MqttBroker::clean_start) 恢复服务器保存的会话，而不是开始一个新会话。
[`session_expiry`](api:ruststream_rumqttc::MqttBroker::session_expiry) 是服务断开后服务器保留会话的时长，这里是一天。
服务停机期间，服务器为它排队保存与其订阅匹配的 `QoS` 1 和 2 消息。
服务以同一个客户端 ID 再次连接时，服务器首先发送这些消息。
Broker 一直暂存它们，直到它们所属的订阅打开，所以启动时一条也不会丢失。
客户端 ID 是会话的名字，所以服务的每个进程都需要自己的 ID；[连接生产环境](production.md)展示了每个部署一个 ID 的做法。

默认情况下，Mosquitto 不会为不在线的会话排队保存 `QoS` 0 消息。
服务停机期间发布的采样就此消失，这正是实时图表需要的。

## 流量控制 {/* #flow-control */}

[`receive_maximum(100)`](../components/broker/limits.md) 告诉服务器，服务在确认一条投递之前最多接收多少个 `QoS` 1 和 2 投递。
达到上限后，服务器停止发送，直到某个处理器结算了一条投递。
默认值是 1000。
未确认的投递在连接结束之前一直占着窗口中的位置，所以每个没有声明上限的 `retry` 都会占掉一个位置，直到下一个会话。
`QoS` 0 投递没有这样的窗口：比发布者慢的处理器会让消息在内存中无限制地排队。

## 测试这些保证 {/* #testing-the-guarantees */}

进程内测试检查 `bill` 对每份报告作何结算，以及同时发布的三个采样作为一个批到达。
会话、会话的恢复和 Receive Maximum 窗口都存在于服务器上，所以有一个测试在 Mosquitto 上运行：

```bash
MQTT_TEST_URL=mqtt://localhost:1883 cargo test -- --test-threads=1
```

它启动服务，突然断开服务的连接，在服务停机期间发布一份用电报告，然后以同一个客户端 ID 再次启动服务。
服务停机期间发布的报告，是 `bill` 收到的第一份报告。
没有 `MQTT_TEST_URL` 时，测试立即返回。
[测试 MQTT 服务](testing.md)展示了这种模式。
