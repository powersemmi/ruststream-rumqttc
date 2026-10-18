---
title: 设计
sidebar_position: 2
description: "ruststream-rumqttc 的位置：在 RustStream 运行时与 rumqttc 客户端之间，使用一个由它自己的任务驱动的连接。"
---

import {Diagram} from '@site/src/components/Diagrams';

export const words = {
  "title": "自上而下依次是服务、RustStream 运行时、本 crate、rumqttc 客户端和 MQTT 服务器",
  "text": "以文字阅读本图",
  "service": "你的服务",
  "handlers": ["#[subscriber]", "订阅者处理器"],
  "app": ["RustStream::new(..)", ".with_broker(..)"],
  "core": "RustStream 核心",
  "runtime": "运行时：分发、工作分区、结算、重试",
  "crate": "ruststream-rumqttc",
  "subscriber": ["MqttSubscriber", "产出 MqttMessage"],
  "publishers": ["MqttPublisher", "由策略生成"],
  "context": "上下文：DeliveryTopic",
  "inProcess": "进程内传输",
  "broker": ["MqttBroker", "一个连接"],
  "descriptors": ["MqttTopic,", "MqttFilter"],
  "task": ["连接任务：", "转交每个 PUBLISH"],
  "client": "rumqttc",
  "eventLoop": ["EventLoop", "负责读写"],
  "asyncClient": ["AsyncClient", "接收请求"],
  "server": "MQTT 服务器",
  "mounted": "挂载到",
  "polls": "轮询流",
  "drives": "轮询"
};

# 设计

`ruststream-rumqttc` 所处的位置：在 RustStream 运行时与 `rumqttc` 客户端之间，使用一个由它自己的任务驱动的连接。

<Diagram name="design" words={words} />

本 crate 在 [`rumqttc`](https://docs.rs/rumqttc/0.25.1/rumqttc/v5/) 之上实现核心的 Broker trait，只使用 MQTT 5。它上面的运行时对每个 Broker 都相同。MQTT 特有的一切都在本 crate 里：[描述符](../components/subscriptions/index.md)、[ack 发送什么](../components/settlement/index.md)、[重试副本发往何处](../components/settlement/retries.md)、[投递的主题](../components/context/index.md)，以及消息头到 MQTT 5 属性的[映射](../components/publishing/headers.md)。消息头使用协议自己的属性，因此用任何语言编写的对端读到的都是普通的 MQTT 消息。

`rumqttc` 把一个连接分成两半。`AsyncClient` 是一个可克隆的句柄，负责把请求排入队列：订阅、发布、确认。`EventLoop` 把这些请求写入套接字，并读取服务器发来的内容，而且只在有人轮询它时才做这两件事。keep-alive 的 ping 和确认的发出，也都靠轮询来推动。

已连接的 Broker 持有一对这样的对象。本 crate 在连接时启动一个自己的任务，在上面轮询事件循环。这个任务只做轮询和转交。因此慢速处理器永远不会阻塞连接：ping、确认以及其他订阅的投递都照常进行。订阅、发布和确认通过 `AsyncClient` 进行，在哪个任务里调用，就在哪个任务里执行。

连接任务把服务器发来的每个 PUBLISH 交给它所属的订阅，每个订阅一个通道。Broker 的所有订阅和发布者共用这一个连接，所以一个有许多处理器的服务在服务器上只持有一个会话。[多路分解](demultiplexing.md)一页用图说明一个连接如何区分它的订阅。

各部分如何启动和停止，见[生命周期](lifecycle.md)；一条消息的路径，见[路径](path.md)。

## 进程内传输 {/* #the-in-process-transport */}

启用 `testing` 特性后，[`TestApp::start`](../components/testing/index.md) 把同一个 Broker 连接到进程内传输，而不是服务器。描述符、策略和投递都与生产环境相同，传输读取 Broker 自己的设置。发布经过与线上相同的校验，消息头也以相同的方式映射到属性上。

这个传输模拟的是该连接所对接的服务器。它用 `+` 和 `#` 匹配过滤器，并遵守 `$` 规则。它以较低的服务质量为每个匹配的订阅发送一个报文，并带上该订阅的标识符，每个报文都经过连接任务所执行的同一套多路分解。因此，共享组的成员之间只取一份副本，重叠的过滤器各自收到属于自己的一份。它保存每个主题的保留消息，丢弃大于 `max_packet_size` 的消息，并在关闭后拒绝确认。

属于服务器和第二个连接的行为只在[线上](../components/testing/live.md)运行：持久会话及其恢复时的重新投递、遗嘱消息、Receive Maximum 窗口、QoS 1 和 QoS 2 握手、访问控制，以及一台服务器上的多个连接。每个以进程内方式连接的 Broker 都是一台独立的服务器。

不启用该特性时，传输不会被编译。生产构建中的连接句柄与它所包装的 `AsyncClient` 大小完全相同，编译器会检查这个大小。
