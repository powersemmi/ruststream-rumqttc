---
title: 发布命令
sidebar_position: 5
description: "向众多设备中的一台发送命令，按消息选择服务质量，并把消息头作为任何语言的对端都能读取的 MQTT 5 属性传递。"
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# 发布命令

本指南向众多设备中的一台发送命令，每条命令使用它所需的服务质量，并带上用任何语言编写的设备都能读取的消息头。

示例是一个指令服务。
操作员向 `operators/commands` 发送命令请求。
服务把每条命令转发到其设备的主题 `devices/<id>/commands`。

## 每台设备一个主题 {/* #a-topic-per-device */}

<Tabs groupId="api">
<TabItem value="macros" label="宏">

```rust title="src/main.rs" reference="examples/guide/publishing.rs#command"
```

</TabItem>
<TabItem value="manual" label="手写">

```rust title="src/main.rs" reference="examples/manual/guide/publishing.rs#command"
```

</TabItem>
</Tabs>

`DeviceCommand` 的主题是一个名字模板。
`{device}` 变成发布上的一个 setter，所以处理器指定设备，主题随之确定。
`Commands` 是处理器用来发布的槽位，它只发布 `DeviceCommand`。

## 命令 {/* #the-command */}

<Tabs groupId="api">
<TabItem value="macros" label="宏">

```rust title="src/main.rs" reference="examples/guide/publishing.rs#dispatch"
```

</TabItem>
<TabItem value="manual" label="手写">

```rust title="src/main.rs" reference="examples/manual/guide/publishing.rs#dispatch"
```

</TabItem>
</Tabs>

槽位的策略为每条命令提供服务质量。
紧急命令使用 [`qos` 步骤](../components/publishing/mqtt-publish-steps.md)，改以 `QoS` 2 发出。
步骤只改变一条消息；调用没有指定的设置取自策略。
该步骤写入 [`MqttPublishOptions`](api:ruststream_rumqttc::MqttPublishOptions)，所以槽位的约束要写明这个类型，文件也要导入 Broker 的 prelude。
另一个步骤是 `retain`，[设备状态](device-state.md)把它用作策略。

`operator` 消息头作为 MQTT 5 [用户属性](../components/publishing/headers.md)传递。
设备把它当作普通属性读取，载荷外面没有任何信封。
`content-type`、`reply-to` 和 `correlation-id` 这几个消息头则使用协议自己的属性：内容类型、响应主题和关联数据。

`publish` 在客户端接管消息后就返回，不等服务器确认。
在 `QoS` 1 和 2 下，客户端会一直重发，直到服务器确认为止，重连之后也是如此。
`publish` 返回错误，意味着客户端没有接管这条消息：连接已经关闭，或者消息违反了协议的规则。
带 `+` 或 `#` 的主题就违反了规则：请求把设备命名为 `+`，主题变成 `devices/+/commands`，`dispatch` 丢弃这个请求。
带控制字符的消息头或主题也以同样的方式被拒绝，因为服务器可能因此关闭连接。

## 内容类型 {/* #the-content-type */}

<Tabs groupId="api">
<TabItem value="macros" label="宏">

```rust title="src/main.rs" reference="examples/guide/publishing.rs#app"
```

</TabItem>
<TabItem value="manual" label="手写">

```rust title="src/main.rs" reference="examples/manual/guide/publishing.rs#app"
```

</TabItem>
</Tabs>

挂载点给槽位指定策略 `QoS` 1 和一个发布转换。
`Json` 在槽位发送的每条命令上加上 `content-type: application/json`。
Broker 把它变成内容类型属性，并设置载荷格式指示符，于是设备把消息体当作 UTF-8 文本读取。
编解码器自己不写这个消息头：服务在挂载点用一个转换一次性声明自己的媒体类型。

## 测试命令 {/* #testing-the-commands */}

示例文件中的测试在进程内运行应用，并读取槽位发布的命令。
`tb.out::<Commands>()` 显示每条消息的设置：普通命令没有设置，紧急命令是 `QoS` 2。
`devices/dev42/commands` 上的 `published` 显示载荷和两个消息头。
进程内服务器像客户端那样校验主题和消息头，所以设备 `+` 的请求在那里同样被丢弃。
