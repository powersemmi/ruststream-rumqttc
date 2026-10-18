---
title: 路径
sidebar_position: 4
description: "一条 MQTT 消息在 RustStream 服务中的路径：事件循环、连接任务、投递、处理器、应答和确认。"
---

import {Diagram} from '@site/src/components/Diagrams';

export const words = {
  "title": "一条需要应答的消息的路径：沿左列从服务器下行到处理器，再沿右列上行，回到网络上",
  "text": "以文字阅读本图",
  "poll": ["EventLoop::poll", "解码 PUBLISH"],
  "demux": ["连接任务", "找到所属的订阅"],
  "message": ["MqttMessage", "共享载荷，不复制"],
  "dispatch": ["分发", "一个循环，或工作分区"],
  "decode": ["解码", "从载荷读取"],
  "handler": ["订阅者的处理器", "#[subscriber] 下的 async fn"],
  "server": ["MQTT 服务器"],
  "write": ["EventLoop 写出", "PUBLISH，然后 PUBACK"],
  "requests": ["AsyncClient 的", "请求通道"],
  "ack": ["确认", "PUBACK 或 PUBREC"],
  "publisher": ["MqttPublisher", "交出缓冲区"],
  "encode": ["应答", "编码进缓冲区"],
  "then": "然后"
};

# 路径

一条 MQTT 消息在 RustStream 服务中的路径：事件循环、连接任务、投递、处理器、应答和确认。

<Diagram name="path" words={words} />

连接任务轮询 `rumqttc` 的事件循环，事件循环从套接字读取 PUBLISH 并解码。任务读取主题，找到该报文所属的订阅（见[多路分解](demultiplexing.md)一页的图），并通过每个订阅自己的通道把投递发给它。不是 UTF-8 的主题会被丢弃，并记录一条警告。

[`MqttMessage`](api:ruststream_rumqttc::MqttMessage) 与报文共享载荷，因此消息体从不复制。它持有主题的一份副本，以及由报文属性构建的消息头。QoS 1 或 QoS 2 投递还保留报文的标识，确认时要用到它。处理器读取的[上下文](../components/context/index.md)同样持有主题。

订阅的通道本身没有上限。对于 QoS 1 和 QoS 2，上限由服务器决定：它发出的未确认投递不超过 [Receive Maximum](mqtt/qos.md)。不匹配任何已打开订阅的报文会等待之后打开的订阅，最多 1024 个。

处理器的应答编码进一个缓冲区，[发布者](../components/publishing/index.md)把这个缓冲区原样交给 `rumqttc`，不做复制。消息头成为报文的属性。请求进入客户端的请求通道后，发布就返回，而不是在服务器确认之后。

投递在应答之后结算。它的确认经过同一个请求通道，因此在应答之后发出，并且早于之后关闭时的 DISCONNECT。每种结果发送什么，见[结算](settlement.md)。

[基准测试](benchmarks/index.md#cost-of-the-code)统计的正是这条路径：一次投递和一次应答的指令数和内存分配，包括客户端在服务线程上所做的工作。
