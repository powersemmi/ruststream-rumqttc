---
title: 请求与响应
sidebar_position: 7
description: "在每个 MQTT 5 请求指定的响应主题上带着关联数据作答，请求没有指定时退回到一个固定主题。"
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# 请求与响应

本指南回答 MQTT 5 请求：每个答复都发往请求指定的主题，并带上请求的关联数据。

MQTT 5 为此给请求提供了两个属性。
响应主题说明答复发往何处。
关联数据是一个不透明的值，请求方用它把答复和问题对应起来。
Broker 在两个方向上把它们映射到 `reply-to` 和 `correlation-id` 消息头。

示例是一个设备代理。
控制台在 `devices/dev42/config/get` 上向它查询某一项设置，它以该设置的值作答。

## 应答者 {/* #the-responder */}

<Tabs groupId="api">
<TabItem value="macros" label="宏">

```rust title="src/main.rs" reference="examples/guide/request_response.rs#responder"
```

</TabItem>
<TabItem value="manual" label="手写">

```rust title="src/main.rs" reference="examples/manual/guide/request_response.rs#responder"
```

</TabItem>
</Tabs>

[应答者](../components/publishing/responders.md)就是带回复的普通处理器：它返回的值会被发布。
`ConfigValue` 没有指定主题，所以由挂载点指定：`reply("devices/dev42/config")`。
这个主题是后备主题，用于没有携带响应主题的请求。
设备没有的设置以 `drop` 结束，不发布任何东西。

## 在响应主题上作答 {/* #answering-on-the-response-topic */}

<Tabs groupId="api">
<TabItem value="macros" label="宏">

```rust title="src/main.rs" reference="examples/guide/request_response.rs#transform"
```

</TabItem>
<TabItem value="manual" label="手写">

```rust title="src/main.rs" reference="examples/manual/guide/request_response.rs#transform"
```

</TabItem>
</Tabs>

`ToResponseTopic` 是回复位置上的一个发布转换。
它读取请求的消息头：`reply-to` 成为答复的主题，`correlation-id` 被复制到答复上。
它声明 `Destination = Names`，从而获得选择主题的权利。
没有 `reply-to` 的请求保留挂载点的主题。

服务的 [AsyncAPI 文档](../components/asyncapi/replies.md)把回复的地址报告为请求的 `reply-to` 消息头 `$message.header#/reply-to`，因为答复不在任何固定的通道上。

## 从命令行提问 {/* #asking-from-the-command-line */}

请求方可以是任何 MQTT 5 客户端。
Mosquitto 自带一个：`mosquitto_rr`，它发布带响应主题的请求并打印答复：

```bash
docker compose exec mosquitto mosquitto_rr \
  -t devices/dev42/config/get -e consoles/c1/responses -m '{"key":"firmware"}'
```

它打印 `{"key":"firmware","value":"2.1.0"}`。
RustStream 服务中的请求方发布请求时设置这两个消息头，并用自己响应主题上的订阅者读取答复。

## 测试应答者 {/* #testing-the-responder */}

示例文件中的测试在进程内发布一条带 `reply-to` 和 `correlation-id` 消息头的查询。
这些消息头到达处理器时，经过的是与线上相同的 MQTT 5 属性映射。
测试检查 `consoles/c1/responses` 上带关联数据的答复、没有响应主题的查询所用的后备主题，以及未知设置得不到答复。
