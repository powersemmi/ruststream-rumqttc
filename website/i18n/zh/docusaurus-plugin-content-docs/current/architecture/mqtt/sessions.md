---
title: 会话
sidebar_position: 2
description: "MQTT 会话保存客户端的订阅和尚未确认的消息；持久会话比连接存在得更久，并在重连时恢复。"
---

import {Diagram} from '@site/src/components/Diagrams';

export const words = {
  "title": "一个持久会话：从第一次 CONNECT 到连接丢失后的恢复",
  "text": "以文字阅读本图",
  "serviceLane": "本服务",
  "serverLane": "MQTT 服务器",
  "connect": ["CONNECT：客户端 ID、", "clean start false、", "会话过期间隔、遗嘱"],
  "connack": ["CONNACK", "session present：否"],
  "ping": ["空闲时发送 PINGREQ，", "以维持 keep-alive"],
  "lost": ["连接丢失，", "没有 DISCONNECT"],
  "will": ["服务器发布", "遗嘱消息"],
  "kept": ["服务器保留会话：", "订阅和", "未确认的消息"],
  "reconnect": ["再次 CONNECT：", "相同的客户端 ID，", "clean start false"],
  "present": ["CONNACK", "session present：是"],
  "resend": ["保留下来的消息", "再次发送"]
};

# 会话

MQTT 会话保存客户端的订阅和尚未确认的消息；持久会话比连接存在得更久，并在重连时恢复。

<Diagram name="sessions" words={words} />

客户端 ID 是会话的名字。[`MqttBroker::new`](../../components/broker/index.md) 在 URL 之外接收它。使用同一个 ID 的第二个连接会接管该会话。

CONNECT 中的两个设置决定会话是否比连接存在得更久。[`clean_start(false)`](../../components/broker/session.md) 请求服务器恢复该客户端 ID 已有的会话。`session_expiry` 说明连接结束后服务器保留会话多长时间，以整秒计。不设置它们时，连接开始一个干净的会话，会话随连接一起结束。

CONNACK 回答服务器是否找到了会话。服务器报告没有会话时，本 crate 重新订阅每个过滤器。会话存在时，订阅仍在服务器上，接下来到达的是服务离开期间到达的、或者服务从未确认的 QoS 1 和 QoS 2 消息。这些消息可能在它们所属的订阅打开之前到达，所以本 crate 最多暂存 1024 条，直到订阅打开。

keep-alive 是连接允许的最长静默时间。没有其他数据传输时，客户端发送 PINGREQ。连接的静默时间达到该间隔的一倍半时，服务器结束连接。除非 [`keep_alive`](../../components/broker/reconnect.md) 指定了别的值，`rumqttc` 请求 60 秒，它的最小值是 5 秒。

[遗嘱消息](../../components/broker/last-will.md)是 CONNECT 留给服务器的一条消息。连接在没有 DISCONNECT 的情况下结束时，服务器发布它：进程崩溃、网络中断、keep-alive 超时。[关闭](../../components/broker/shutdown.md)会发送 DISCONNECT，所以正常停止的服务不会发布遗嘱消息。

CONNECT 中的另外两个设置限制服务器发送的内容。Receive Maximum 见[服务质量](qos.md)一页的图。Maximum Packet Size 是客户端接受的最大报文，超过它的消息，服务器不会发给这个客户端，而是直接丢弃。本 crate 默认声明 1 MiB，[`max_packet_size`](../../components/broker/limits.md) 可以修改它。CONNACK 还说明服务器是否提供订阅标识符，[多路分解](../demultiplexing.md)依赖于此。

会话就是协议自己的会话，出自 [MQTT 5.0 标准](https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html)的 3.1、3.2 和 4.1 节。本 crate 如何打开和关闭它，见[生命周期](../lifecycle.md)。
