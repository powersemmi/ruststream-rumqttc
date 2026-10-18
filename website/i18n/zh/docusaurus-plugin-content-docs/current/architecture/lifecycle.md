---
title: 生命周期
sidebar_position: 3
description: "MqttBroker::new 只记录配置，connect 等待服务器的 CONNACK，启动时订阅每个过滤器，关闭时发送 DISCONNECT。"
---

import {Diagram} from '@site/src/components/Diagrams';

export const words = {
  "title": "Broker 的三个状态作为类型，以及每次转换在 MQTT 上做什么",
  "text": "以文字阅读本图",
  "states": "Broker",
  "mqtt": "在 MQTT 上",
  "unconnected": ["MqttBroker::new", "(url, client_id)"],
  "connected": ["ConnectedMqttBroker"],
  "closed": ["Closed: ()", "没有要报告的内容"],
  "config": ["URL、客户端 ID", "和设置，无 I/O"],
  "options": ["检查设置：", "端口、keep-alive、TLS"],
  "dial": ["启动任务，最多等待", "30 秒 CONNACK"],
  "subscribe": ["每个订阅：", "SUBSCRIBE、SUBACK"],
  "running": ["带退避地重连，", "重新订阅"],
  "disconnect": ["标记为已关闭，发送", "DISCONNECT，刷出"],
  "afterShutdown": ["比连接活得更久的发布者", "返回 MqttError::NotConnected，绝不默默成功"],
  "connect": "connect(self)",
  "shutdown": "shutdown(self)"
};

# 生命周期

[`MqttBroker::new`](../components/broker/index.md) 只记录配置，connect 等待服务器的 CONNACK，启动时订阅每个过滤器，关闭时发送 DISCONNECT。

<Diagram name="lifecycle" words={words} />

`new` 不做任何 I/O，所以服务的构建函数保持同步。它接收 URL（`mqtt://host:port` 或 `mqtts://host:port`）和客户端 ID，构建函数的各项设置与它们记录在一起。运行时在应用启动时连接 Broker。

connect 先检查设置，然后才做任何 I/O。端口不是数字、keep-alive 低于客户端 5 秒的下限、启用 TLS 却没有可信任的证书颁发机构，这几种情况都会让启动失败，返回指出相应设置的 [`MqttError::Invalid`](../components/broker/errors.md)。

接着 connect 启动连接任务，最多等待 30 秒，直到服务器发来第一个 [CONNACK](../components/broker/startup.md)。任务会重试时间可能解决的问题，例如服务器尚未启动。连接无法靠重试克服的拒绝会立即结束等待，并返回 `MqttError::Connect`：凭据错误、服务器拒绝的客户端 ID、不使用 MQTT 的对端、证书无法满足的 TLS 握手。等待超时时，返回它看到的最后一个连接错误。

每个订阅的[描述符](../components/subscriptions/index.md)针对已连接的 Broker 解析一次。它发送 SUBSCRIBE 并等待 SUBACK，因此在应用报告启动完成之前，服务器已经持有该订阅。服务器拒绝的过滤器会让启动失败，并返回 `MqttError::Subscribe`。

服务运行期间，任务会[重连](../components/broker/reconnect.md)丢失的连接。第一次尝试前等待 100 毫秒，此后每次等待时间加倍，最长 5 秒。CONNACK 报告会话已不存在时，它重新订阅每个过滤器。连接无法靠重试克服的拒绝则会结束任务，每个订阅都以 `MqttError::Receive` 返回它。

[关闭](../components/broker/shutdown.md)首先把连接标记为已关闭，因此引用它的发布者无法再塞进一条消息。随后它把 DISCONNECT 排在所有已排队的确认之后，并最多等待 5 秒，让任务把它们全部写出。服务器看到的是连接的正常结束，不会发布遗嘱消息。关闭不再返回其他内容：已关闭状态不携带诊断信息。

在关闭之前配对的发布者是关闭无法收回的句柄。它返回 `MqttError::NotConnected`。在关闭之后确认的投递返回一个 Broker 错误。

进程内传输遵循同样的阶梯。它的 connect 像 `connect` 一样检查设置，因此服务器路径拒绝的配置在测试中同样会被拒绝。
