---
title: 服务质量
sidebar_position: 1
description: "MQTT 在 QoS 1 下用一个报文确认一次投递，在 QoS 2 下用四个报文的握手确认；本 crate 在处理器完成后发送确认。"
---

import {Diagram} from '@site/src/components/Diagrams';

export const words = {
  "title": "每种服务质量下的一次投递：服务器与服务之间交换的报文，以及处理器在何处运行",
  "text": "以文字阅读本图",
  "serverLane": "MQTT 服务器",
  "serviceLane": "本服务",
  "qos0": "QoS 0：至多一次",
  "qos1": "QoS 1：至少一次",
  "qos2": "QoS 2：恰好一次",
  "publish0": "",
  "publish1": "",
  "publish2": "",
  "handler0": ["处理器运行"],
  "handler1": ["处理器运行"],
  "handler2": ["处理器运行"],
  "puback": "ack 时",
  "pubrec": "ack 时",
  "pubrel": "",
  "pubcomp": "由 rumqttc 完成"
};

# 服务质量

MQTT 在 QoS 1 下用一个报文确认一次投递，在 QoS 2 下用四个报文的握手确认；本 crate 在处理器完成后发送确认。

<Diagram name="qos" words={words} />

服务质量要选两次：发布者为自己的消息选择，订阅为它接受的内容选择。投递按两者中较低的一个进行，所以 QoS 0 的发布到达 QoS 1 的订阅时仍是 QoS 0。描述符的 [`Qos`](../../components/subscriptions/qos.md) 设置订阅这一侧，[发布策略](../../components/publishing/index.md)设置消息这一侧。

在 QoS 0 下，服务器发出 PUBLISH 之后就不再管它。没有任何东西返回，所以 QoS 0 投递没有可确认的东西：`ack` 返回 `AckError::Unsupported`，运行时把它记入日志。

在 QoS 1 和 QoS 2 下，服务器一直保存消息，直到服务作出应答。本 crate 以手动确认的方式连接，所以 `rumqttc` 把每个 PUBLISH 交给订阅，自己什么也不发送。投递被确认时，也就是处理器返回之后，[确认](../../components/settlement/index.md)才会发出：QoS 1 下是 PUBACK，QoS 2 下是 PUBREC。完成 QoS 2 的 PUBREL 和 PUBCOMP 由 `rumqttc` 自己交换。

MQTT 没有否定确认。服务从未确认的 QoS 1 或 QoS 2 投递留在服务器上。只有当持久会话在新连接上恢复时，服务器才会再次发送它，并标记为重复。[会话](sessions.md)一页用图说明了这种恢复，[结算](../settlement.md)一页说明处理器的每种结果发送什么。

客户端在连接时声明一个 Receive Maximum：它在不确认的情况下最多接收多少个 QoS 1 和 QoS 2 投递。未确认的投递达到这个数目后，服务器在该连接上不再发送 QoS 1 或 QoS 2 消息，直到其中一个被确认。本 crate 默认声明 1000，[`receive_maximum`](../../components/broker/limits.md) 可以修改它。这个窗口就是慢速订阅积压投递的上限。QoS 0 投递不参与其中。

服务发起的发布，在 `rumqttc` 把消息放进它的请求通道后就返回，而不是在服务器确认之后。这个通道容纳 64 个请求。`rumqttc` 保存每个 QoS 1 和 QoS 2 发布，直到服务器确认它。重连后，如果服务器报告会话存在，它会再次发送这些发布。重连时开始了新会话，这些发布就会被丢弃，通道上仍在等待的请求也一样。

这些流程就是协议自己的流程，出自 [MQTT 5.0 标准](https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html)的 4.3 节和 4.9 节。
