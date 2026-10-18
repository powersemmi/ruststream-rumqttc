---
title: 结算
sidebar_position: 6
description: "ack、drop、retry 和 retry_after 在 MQTT 上变成什么：QoS 1 和 2 下的一次确认，QoS 0 下什么也不发送，以及运行时发布的副本。"
---

import {Diagram} from '@site/src/components/Diagrams';

export const table = {
  "title": "QoS 1 或 QoS 2 投递上处理器的每种结果：投递发送什么，以及随后发生什么",
  "text": "以文字阅读本图",
  "outcomeLane": "处理器",
  "wireLane": "发送",
  "effectLane": "随后",
  "ack": ["ack()"],
  "drop": ["drop()"],
  "retry": ["retry()"],
  "retryCapped": ["retry()", "有上限时"],
  "retryAfter": ["retry_after(d)"],
  "unsettled": ["没有结果：", "被放弃的批"],
  "nothing": "什么也不发送",
  "ackEffect": ["完成"],
  "dropEffect": ["完成，", "消息", "被放弃"],
  "retryEffect": ["持久会话", "恢复时", "再次到来"],
  "retryCappedEffect": ["立即发布", "一份副本"],
  "retryAfterEffect": ["d 之后", "发布副本"],
  "unsettledEffect": ["同 retry()：", "恢复时再次到来"],
  "qos0": ["QoS 0 下什么也不发送：ack 和 drop 返回 AckError::Unsupported，", "运行时发布的副本照常发出"]
};

export const retries = {
  "title": "MQTT 上的一次重试：按运行时的消息头计数的尝试上限，决定发布副本还是发往死信主题",
  "text": "以文字阅读本图",
  "delivery": ["一个投递", "从 x-ruststream-retry-count 读取尝试次数"],
  "outcome": ["处理器返回", "retry_after(d)，或在有声明时返回 retry()"],
  "cap": ["max_attempts：这次尝试是否低于上限？"],
  "below": "低于上限",
  "acked": ["原消息", "被确认，", "副本等待 d"],
  "topicCopy": ["MqttTopic：", "副本发往", "它的主题"],
  "filterCopy": ["MqttFilter：", "发往挂载点", "或转换", "指定的主题"],
  "atCap": "到达上限",
  "deadLetter": ["死信主题：", "投递", "发布到那里"],
  "rejected": ["没有死信：", "确认", "并放弃"],
  "fanOut": ["副本就是一次发布：过滤器与其主题匹配的", "每个订阅都会收到它。等待延迟的副本", "保存在进程中，计时器触发前", "崩溃就会丢失它。"],
  "handled": "已处理",
  "retries": "重试",
  "belowCap": "低于上限",
  "reached": "到达上限"
};

# 结算

`ack`、`drop`、`retry` 和 [`retry_after`](../components/settlement/retry-after.md) 在 MQTT 上变成什么：QoS 1 和 2 下的一次确认，QoS 0 下什么也不发送，以及运行时发布的副本。

<Diagram name="settlement" words={table} />

MQTT 对一次投递只有一种应答，就是确认：QoS 1 下是 PUBACK，QoS 2 下是 PUBREC。`ack` 发送它。`drop` 也发送它，因为协议只允许服务以确认的方式结束一条消息。

MQTT 没有否定确认，所以服务器绝不会在当前连接上再次发送消息。没有任何声明的 [`retry()`](../components/settlement/retry.md) 返回 `AckError::Unsupported`，运行时把它记入日志，投递保持未确认。只有当持久会话在新连接上恢复时，服务器才会再次发送它。在此之前，它占用 [Receive Maximum](mqtt/qos.md) 窗口的一个槽位：一旦窗口被这样的投递占满，服务器在该连接上就不再发送 QoS 1 或 QoS 2 消息。处理器放弃的批同样保持未确认。

会话内的重试是运行时发布的一份副本。`retry_after(d)` 确认原消息，等待 `d`，然后发布一份副本。声明了上限或死信主题时，`retry()` 立即发布它的副本。QoS 0 投递没有可确认的东西，它的副本照样发出。

| 结果 | QoS 0 |
|---|---|
| `ack`、`drop` | `AckError::Unsupported` |
| `retry()` | `AckError::Unsupported`；声明了上限时，立即发布一份副本 |
| `retry_after(d)` | 在 `d` 之后发布一份副本 |
| 没有结果 | 什么也不发生 |

## 重试 {/* #retries */}

<Diagram name="retries" words={retries} />

上限在挂载点用 [`max_attempts`](api:ruststream::runtime::Mounting::max_attempts) 声明，第一次投递算作一次。MQTT 自己不统计投递次数，所以由运行时统计它发布的副本，次数记录在每份副本的 `x-ruststream-retry-count` 消息头中。

低于上限时，副本发往订阅形式的地址。[`MqttTopic`](../components/subscriptions/mqtt-topic.md) 的地址是它自己的主题，共享的 `MqttTopic` 也是如此，因此它的注册无需指定任何东西。[`MqttFilter`](../components/subscriptions/mqtt-filter.md) 匹配许多主题，却不能发布到其中任何一个，所以它的注册要[指定副本发往的主题](../components/settlement/out-retry.md)：可以只指定一次，也可以通过转换为每次投递指定。两者都没有指定的过滤器注册无法启动。

到达上限时，投递发往[死信主题](../components/settlement/dead-letters.md)，载荷不变。没有声明死信主题时，投递被确认并放弃。

副本是一次普通的发布，因此它会到达过滤器与其主题匹配的每个订阅，本服务和其他服务中的订阅都包括在内。被某个活动过滤器匹配的死信主题会把消息直接送回来，所以死信主题应取一个没有订阅读取的名字。等待延迟的副本保存在进程中：正常关闭会等待它，计时器触发前崩溃则会丢失它。
