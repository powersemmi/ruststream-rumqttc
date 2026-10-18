---
title: 重试与死信主题
sidebar_position: 6
description: "立即或稍后重试一条 MQTT 消息，限定尝试次数，把一直失败的消息转入死信主题，并把过滤器的副本送回各自的主题。"
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# 重试与死信主题

本指南立即或稍后重试一条消息，限定尝试次数，并把一直失败的消息转入[死信主题](../components/settlement/dead-letters.md)。

MQTT 既不扣留消息，也不统计投递次数，也没有死信队列。
这三件事都由框架来做：挂载点声明上限和死信主题，重试以消息副本的形式传递，消息头里带着重试次数。

示例是一个固件服务。
它在设备上安装更新，而设备可能在一段时间内拒绝更新。
它还存储每台设备发来的报告。

## 订阅者 {/* #the-subscriber */}

设备有三种拒绝更新的方式，每种需要不同的结果。
`Busy` 表示设备正在执行任务：现在提供更新仍会被拒绝，所以 [`retry_after`](../components/settlement/retry-after.md) 在 30 秒后再提供。
`Interrupted` 表示传输中断：[`retry`](../components/settlement/retry.md) 立即再试一次。
`Incompatible` 表示镜像不适合这台设备，再试一次也是同样的答复，所以这次更新被丢弃。

<Tabs groupId="api">
<TabItem value="macros" label="宏">

```rust title="src/main.rs" reference="examples/guide/retries.rs#install"
```

</TabItem>
<TabItem value="manual" label="手写">

```rust title="src/main.rs" reference="examples/manual/guide/retries.rs#install"
```

</TabItem>
</Tabs>

`retry_after` 确认这次投递，等待，然后把一份副本发布回 `updates/install`。
副本带着重试次数消息头（[`RETRY_COUNT_HEADER`](api:ruststream::runtime::RETRY_COUNT_HEADER)），其值加一。
它在每种服务质量下都有效；`QoS` 0 投递没有可确认的东西，但副本照样发布。
副本在服务的内存中等待：在延迟结束前停止的进程会丢失它。

`retry` 也会变成一份副本，并立即发布，因为下面的挂载点声明了上限。
MQTT 不统计重新投递，所以上限通过副本的消息头来计数。
没有上限时，`retry` 让投递保持未确认状态，留给下一个会话，[投递保证](delivery.md)展示了这一点。

## 上限与死信主题 {/* #the-cap-and-the-dead-letter-topic */}

<Tabs groupId="api">
<TabItem value="macros" label="宏">

```rust title="src/main.rs" reference="examples/guide/retries.rs#app"
```

</TabItem>
<TabItem value="manual" label="手写">

```rust title="src/main.rs" reference="examples/manual/guide/retries.rs#app"
```

</TabItem>
</Tabs>

[`max_attempts`](../components/settlement/retries.md) 统计一次更新的每次投递，第一次也算在内。
第四次投递不再重试：[`dead_letter`](api:ruststream::runtime::RouterWith::dead_letter) 把它发布到 `updates/dead`，载荷和消息头保持到达时的样子。
被丢弃的更新完成结算，不会去任何地方；死信主题只存放用完了尝试次数的更新。

副本是一次普通的发布，所以过滤器与其主题匹配的每个订阅都会收到它，无论在本服务中还是在其他服务中。
因此，每个重试主题和每个死信主题都不在服务读取的任何其他过滤器范围之内。
如果在 `install` 旁边有一个 `updates/#` 订阅，它会读到每次更新的每份副本，以及每封死信。

## 读取死信主题 {/* #reading-the-dead-letter-topic */}

<Tabs groupId="api">
<TabItem value="macros" label="宏">

```rust title="src/main.rs" reference="examples/guide/retries.rs#review"
```

</TabItem>
<TabItem value="manual" label="手写">

```rust title="src/main.rs" reference="examples/manual/guide/retries.rs#review"
```

</TabItem>
</Tabs>

死信主题是一个普通主题，`review` 像读取其他主题一样读取它。
转入死信的更新，其重试次数消息头保存它经历的投递次数，这里是 4。

## 过滤器上的重试 {/* #retries-over-a-filter */}

<Tabs groupId="api">
<TabItem value="macros" label="宏">

```rust title="src/main.rs" reference="examples/guide/retries.rs#reports"
```

</TabItem>
<TabItem value="manual" label="手写">

```rust title="src/main.rs" reference="examples/manual/guide/retries.rs#reports"
```

</TabItem>
</Tabs>

[`MqttTopic`](../components/subscriptions/mqtt-topic.md) 上投递的副本回到那个主题。
[`MqttFilter`](../components/subscriptions/mqtt-filter.md) 匹配许多主题，却不指定其中任何一个，所以它的注册要指定副本的去向，否则服务拒绝启动。
[`out_retry(Publish::default()).to(..)`](../components/settlement/out-retry.md) 为所有副本指定一个主题。
`ToDeliveryTopic` 则为每次投递各指定一个：它从 [MQTT 上下文](../components/context/index.md)读取报告到达时所在的主题，把副本送回那里。
因此，来自 `devices/dev7/reports` 的报告回到 `devices/dev7/reports`，并且只交给 `record`。
处理器接收 `Ctx<DeliveryTopic>`，它为注册提供 MQTT 上下文；在手写形式中，它的上下文类型是 `MqttContext`。

## 测试延迟 {/* #testing-the-delays */}

示例文件中的测试在暂停的时钟上于进程内运行应用，所以 `tb.advance(BACKOFF)` 让 30 秒瞬间过去。
测试统计每次更新的投递次数，读取每份副本的重试次数消息头，并检查什么到达了 `updates/dead`。
[测试 MQTT 服务](testing.md)讲解了测试套件。
