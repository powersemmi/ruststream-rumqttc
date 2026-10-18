---
title: 基准测试
description: "ruststream-rumqttc 在每条消息上的开销：与裸 rumqttc 客户端对照的吞吐量，以及它自身代码的指令数和内存分配次数。"
---

import {Environment, MovedSections, Results} from '@site/src/components/Benchmarks';
import {mqtt} from '@site/src/components/Benchmarks/mqtt';

export const labels = {"loading": "正在读取结果...", "scenario": "场景", "raw": "裸客户端", "adapter": "Broker crate", "framework": "RustStream", "overhead": "额外开销", "against": "（比客户端 {percent}）", "indistinguishable": "无法区分", "brokerBound": "受 Broker 限制", "measured": "测量于", "details": "这些数字是怎样测得的", "pending": "尚未公布结果：{brokers}。", "instructions": "指令数", "allocations": "内存分配", "cold": "冷启动", "table": "以表格显示数字", "axis": "各循环比裸客户端最佳一轮低多少", "rounds": "各轮，从最佳到最慢", "best": "最佳一轮", "median": "中位一轮", "noise": "轮次的波动范围", "comparison": "与裸客户端对照", "code": "代码的开销", "broker": "Broker", "plugin": "{plugin} 插件", "pluginRaw": "不用插件", "pluginAdapter": "手写的插件", "pluginFramework": "crate 自带的插件", "pluginAxis": "各循环比不用插件的应用最佳一轮低多少", "pluginAgainst": "（比不用插件 {percent}）", "pluginOverhead": "比手写插件", "pluginOverHand": "（比手写插件 {percent}）"};

export const machine = {"cpu": "处理器", "architecture": "架构", "cpu_frequency": "频率", "cores": "核心数", "memory": "内存", "memory_speed": "内存速度", "os": "操作系统", "broker": "Broker", "rustc": "编译器", "profile": "构建配置", "features": "特性", "rustflags": "RUSTFLAGS", "load": "平均负载", "round_trip": "往返时间", "valgrind": "Valgrind"};

# 基准测试

`ruststream-rumqttc` 在每条消息上的开销：与裸 `rumqttc` 客户端对照的吞吐量，以及它自身代码的指令数和内存分配次数。

## 与裸客户端对照 {/* #against-a-raw-client */}

第一张图在真实的 MQTT 服务器上把每个场景用三种方式运行。`裸客户端` 直接驱动 `rumqttc`。`Broker crate` 用一个自己的循环驱动本 crate 的订阅，上面没有运行时。`RustStream` 是用户写的服务：应用里的一个 `#[subscriber]` 处理器。第一项差值是本 crate 比它所封装的客户端多付出的开销，第二项是运行时在其上增加的开销。

每个循环画成一条横条，从最佳一轮延伸到最慢的一轮，圆点标出最佳一轮，缺口标出中位一轮。坐标轴的零点是裸客户端的最佳一轮，圆点在零点右侧表示更慢。空心圆点表示该循环与裸客户端的差异小于各轮的波动范围。图表下方的表格列出最佳一轮，括号里是中位数。

两个场景就是服务读取消息时使用的两种[服务质量](../mqtt/qos.md)。在 QoS 0 下，服务器发送每条消息，不结算任何东西。在 QoS 1 下，服务用 PUBACK [确认](../../components/settlement/index.md)每个投递，[路径](../path.md)一页用图说明了这一点。

## 代码的开销 {/* #cost-of-the-code */}

第二张图统计一条消息在服务线程上的开销：稳态下 callgrind 测得的指令数和 DHAT 测得的内存分配次数。每个场景都是一个基于 [`MqttBroker`](../../components/broker/index.md)、在同一台服务器上读取 QoS 1 主题的服务。服务运行在一个线程上，轮询 `rumqttc` 事件循环的任务也运行在这个线程上，所以计数包括框架、本 crate 以及客户端自己在套接字上的工作，PUBACK 也在其中。生产者的线程不计入。`冷启动` 是连接、订阅并取得第一条消息一次性付出的开销。

## 结果 {/* #results */}

<Results entry={mqtt} labels={labels} details="/architecture/benchmarks/methodology/" />

## 机器 {/* #the-machine */}

<Environment entry={mqtt} labels={machine} />

数字来自本版本的 `docs/benchmarks/results.json`，由基准测试命令写入。[方法论](methodology.md)说明它们是怎样测得的，[数字](numbers.md)说明可以从中得出什么结论。

<MovedSections />
