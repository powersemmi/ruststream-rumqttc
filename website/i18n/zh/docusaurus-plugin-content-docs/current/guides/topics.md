---
title: 主题、通配符与共享订阅
sidebar_position: 2
description: "设计 MQTT 主题，用 + 和 # 通配符读取多个主题，在共享组中让服务的多个实例分担工作，并在一个服务中挂载相互重叠的过滤器。"
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# 主题、通配符与共享订阅

本指南逐个读取 MQTT 主题，也一次读取多个主题；它让服务的多个实例分担工作，并挂载相互重叠的过滤器。

主题是由 `/` 分隔的若干层级组成的名字，例如 `devices/dev42/telemetry`。
服务器把消息交给此刻过滤器与其主题匹配的每个订阅。
服务器不为以后保存任何东西，只有两个例外：每个主题一条保留消息（[设备状态](device-state.md)），以及持久会话的队列（[投递保证](delivery.md)）。

示例是一个设备群服务。
它存储每台设备的遥测数据，跟踪某一台设备的每条消息，并安排固件推送。

## 共享组 {/* #a-share-group */}

`store` 读取每台设备的遥测数据。
服务以多个实例运行，每条读数只能存储一次。

<Tabs groupId="api">
<TabItem value="macros" label="宏">

```rust title="src/main.rs" reference="examples/guide/topics.rs#store"
```

</TabItem>
<TabItem value="manual" label="手写">

```rust title="src/main.rs" reference="examples/manual/guide/topics.rs#store"
```

</TabItem>
</Tabs>

[`MqttFilter`](../components/subscriptions/mqtt-filter.md) 读取过滤器匹配的每个主题。
`+` 恰好匹配一个层级，所以 `devices/+/telemetry` 读取每台设备的遥测数据，不会读取更深的层级。
[`shared("ingest")`](../components/subscriptions/shared.md) 订阅 `$share/ingest/devices/+/telemetry`，服务器把每条消息交给该组中的一个会话。
每个实例用自己的客户端 ID 连接，并加入同一个组，所以各实例分摊这些读数。
组外的订阅，或者另一个组中的订阅，仍会得到自己的一份。

处理器看不到 `$share/..` 前缀，也看不到过滤器。
[`Ctx<DeliveryTopic>`](../components/context/index.md) 是这条读数到达时所在的主题，例如 `devices/dev42/telemetry`，所以设备的 id 唾手可得。

## 按设备保持顺序 {/* #order-per-device */}

服务器把共享组的每条读数交给任意一个成员，因此同一设备的读数可能落到不同的副本上。
按设备的顺序只在一个订阅内部成立：[`workers(n, by_key)`](../components/subscriptions/lanes.md) 让键相同的读数依次执行，不同的键并行执行。
键是 `partition-key` 头，即 [`PARTITION_KEY_HEADER`](api:ruststream_rumqttc::PARTITION_KEY_HEADER)，设备在每条读数上设置它，例如自己的 id。
需要按设备保持读数顺序的订阅，应在共享组之外读取。

## 多层通配符 {/* #a-multi-level-wildcard */}

`trace` 把某一台设备的每条消息记入日志，无论消息属于哪一类。

<Tabs groupId="api">
<TabItem value="macros" label="宏">

```rust title="src/main.rs" reference="examples/guide/topics.rs#trace"
```

</TabItem>
<TabItem value="manual" label="手写">

```rust title="src/main.rs" reference="examples/manual/guide/topics.rs#trace"
```

</TabItem>
</Tabs>

`#` 匹配主题的剩余部分，层级数不限，它只能位于过滤器的末尾。
`devices/dev42/#` 读取 `devices/dev42/telemetry`、`devices/dev42/telemetry/raw` 和这台设备的其他所有主题。
设备发送多种消息，所以 `trace` 按原样把载荷当作字节接收，不经过任何编解码器。

以 `$` 开头的主题，例如服务器自己的 `$SYS/..` 统计信息，只有同样以 `$` 开头的过滤器才能匹配。
因此单独的 `#` 永远读不到它们。

## 单个主题 {/* #one-topic */}

<Tabs groupId="api">
<TabItem value="macros" label="宏">

```rust title="src/main.rs" reference="examples/guide/topics.rs#firmware"
```

</TabItem>
<TabItem value="manual" label="手写">

```rust title="src/main.rs" reference="examples/manual/guide/topics.rs#firmware"
```

</TabItem>
</Tabs>

`#[subscriber(..)]` 中的裸字符串是一个主题，按 [`MqttTopic`](../components/subscriptions/mqtt-topic.md) 的默认设置读取。
`MqttTopic::new("fleet/firmware").qos(..)` 写出的是带有设置的同一个订阅。
`MqttTopic` 不接受通配符：名字里带通配符时，启动会在任何 I/O 之前失败，错误信息会指出 `MqttFilter`。
通配符只属于订阅，所以一次发布只指定一个主题，向带 `+` 或 `#` 的主题发布会返回错误。

## 应用 {/* #the-app */}

<Tabs groupId="api">
<TabItem value="macros" label="宏">

```rust title="src/main.rs" reference="examples/guide/topics.rs#app"
```

</TabItem>
<TabItem value="manual" label="手写">

```rust title="src/main.rs" reference="examples/manual/guide/topics.rs#app"
```

</TabItem>
</Tabs>

过滤器匹配许多主题，所以其投递的延迟副本没有唯一的主题可去。
挂载点用 [`out_retry(Publish::default()).to(..)`](../components/settlement/out-retry.md) 指定一个主题，否则服务拒绝启动。
副本会到达过滤器与其主题匹配的每个订阅，所以这里的每个主题只被它自己的过滤器匹配，不被其他过滤器匹配。
[重试与死信主题](retries.md)讲解这些副本。

## 相互重叠的过滤器 {/* #overlapping-filters */}

`store` 和 `trace` 都匹配 `devices/dev42/telemetry`，它们共用一个连接。
该主题上的一条读数，会让它们各运行一次。
服务器为每个匹配的订阅发送一份副本，Broker 根据每份副本携带的[订阅标识符](../components/subscriptions/overlaps.md)区分它们。
对服务器来说，订阅标识符是可选功能。
在不提供它的服务器上，服务启动失败，错误信息会指出这两个过滤器。

同一个过滤器在一个服务中挂载两次，对服务器来说就是一个订阅。
它的处理器轮流处理投递，而不是各得一份副本。

## 测试路由 {/* #testing-the-routing */}

示例文件中的测试在进程内运行应用。
它们向 `devices/dev42/telemetry`、`devices/dev7/telemetry` 和 `devices/dev42/telemetry/raw` 发布消息，并检查哪些处理器运行了。
进程内服务器像 Mosquitto 那样匹配主题，包括通配符和重叠。
多个实例共享一个组，需要多个连接连到同一个服务器，所以分摊本身只能在真实 Broker 上看到。
