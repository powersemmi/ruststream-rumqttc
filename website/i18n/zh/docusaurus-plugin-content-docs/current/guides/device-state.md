---
title: 设备状态：保留消息与遗嘱消息
sidebar_position: 4
description: "用保留消息把每台设备的当前状态保存在 MQTT 服务器上，清除它，并用遗嘱消息公布服务是否在线。"
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# 设备状态：保留消息与遗嘱消息

本指南把每台设备的当前状态保存在服务器上，让稍后启动的读者立即看到它，并公布服务本身是否在运行。

MQTT 不保存历史，只有一个例外：[保留消息](../components/publishing/retained.md)。
服务器保存每个主题的最后一条保留消息，并把它发给过滤器匹配的每个新订阅。
[遗嘱消息](../components/broker/last-will.md)是一条由服务器发布的消息，发布时机是客户端的连接没有经过正常断开就结束了。

示例是一个网关监控服务。
网关发送带电池电量的心跳。
服务把每个网关的状态（`ok` 或 `low-battery`）作为保留消息保存在 `gateways/<id>/state` 上，仪表盘从那里读取。

## 每个网关的状态 {/* #the-state-of-each-gateway */}

<Tabs groupId="api">
<TabItem value="macros" label="宏">

```rust title="src/main.rs" reference="examples/guide/device_state.rs#state"
```

</TabItem>
<TabItem value="manual" label="手写">

```rust title="src/main.rs" reference="examples/manual/guide/device_state.rs#state"
```

</TabItem>
</Tabs>

状态是仪表盘原样显示的纯文本，所以 `GatewayState` 派生 `Serialized`，自带字节，不经过任何编解码器。
它的主题是一个名字模板：`{gateway}` 变成一个 setter，所以 `.to().gateway(..)` 指向某一个网关的主题。
`track` 发布每次心跳所来自的网关的状态。

网关停止服役时，`forget` 向它的状态主题发布一个空载荷。
空的保留消息会删除其主题的保留消息，于是服务器不再把该状态交给新的订阅者。

## 保留发布 {/* #retained-publishes */}

<Tabs groupId="api">
<TabItem value="macros" label="宏">

```rust title="src/main.rs" reference="examples/guide/device_state.rs#app"
```

</TabItem>
<TabItem value="manual" label="手写">

```rust title="src/main.rs" reference="examples/manual/guide/device_state.rs#app"
```

</TabItem>
</Tabs>

[发布策略](../components/publishing/index.md)上的 [`retain(true)`](api:ruststream_rumqttc::MqttPublish::retain) 让经由该槽位的每次发布都成为保留消息。
两个处理器共用槽位 `States` 和这个策略，所以状态和它的删除走同一条路。
也可以只让一条消息成为保留消息：发布上的 [`retain` 步骤](../components/publishing/mqtt-publish-steps.md)在这次调用中覆盖策略。

共享订阅在订阅时不会收到保留消息。
读取状态的仪表盘在任何共享组之外订阅。

## 在线与离线 {/* #online-and-offline */}

<Tabs groupId="api">
<TabItem value="macros" label="宏">

```rust title="src/main.rs" reference="examples/guide/device_state.rs#status"
```

</TabItem>
<TabItem value="manual" label="手写">

```rust title="src/main.rs" reference="examples/manual/guide/device_state.rs#status"
```

</TabItem>
</Tabs>

[`last_will`](api:ruststream_rumqttc::MqttBroker::last_will) 接收遗嘱消息的主题、载荷、服务质量和保留标志。
Broker 在连接时把遗嘱消息交给服务器。
如果连接在没有 `DISCONNECT` 报文的情况下结束，例如进程崩溃或网络故障，服务器就发布遗嘱消息：这里是 `offline`，作为保留消息发布到 `services/gateway-monitor/status`。
正常关闭会发送 `DISCONNECT`，服务器随之丢弃遗嘱消息。

另一半是上面应用中的 `after_startup` 钩子。
它在 Broker 连接后运行一次，把 `online` 作为保留消息发布到同一个主题。
因此状态主题始终保存两者中最新的那一个，订阅它的仪表盘立即就能读到。

## 测试状态 {/* #testing-the-state */}

进程内测试检查服务发布的内容：每个网关的状态、被移除网关的空载荷，以及启动时的 `online`。
进程内服务器像 Mosquitto 那样保存保留消息。
遗嘱消息由服务器为突然结束的连接发布，所以有一个测试在 Mosquitto 上运行：

```bash
MQTT_TEST_URL=mqtt://localhost:1883 cargo test -- --test-threads=1
```

它运行服务，正常停止服务，然后启动一个仪表盘：仪表盘收到 `online` 和网关的状态，两者都是保留消息。
接着它让一次服务运行在没有 `DISCONNECT` 的情况下结束，仪表盘收到 `offline`。
最后它移除测试用的网关，从而清除服务器上该网关的状态。
