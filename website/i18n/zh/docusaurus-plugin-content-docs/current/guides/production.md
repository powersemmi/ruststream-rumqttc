---
title: 连接生产环境
sidebar_position: 9
description: "把 MQTT 服务连接到托管的 Broker：凭据、带客户端证书的 TLS、每个进程一个客户端 ID、限制、重连，以及对配置的测试。"
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# 连接生产环境

托管的 MQTT 服务，例如 AWS IoT Core、HiveMQ Cloud 或 EMQX Cloud，要求[凭据和 TLS](../components/broker/security.md)，往往还要求客户端证书。
本指南从部署环境中读取设置，为这样的服务配置 Broker。

## 凭据与 TLS {/* #credentials-and-tls */}

{/* single-form: the broker's configuration is the same code with and without the macros */}
```rust title="src/main.rs" reference="examples/guide/production.rs#broker"
```

部署环境通过环境变量提供每一项设置，通过文件提供证书。
[`credentials`](api:ruststream_rumqttc::MqttBroker::credentials) 设置 `CONNECT` 报文中的用户名和密码。
URL 中的登录信息（例如 `mqtt://user:p%40ss@host`）同样有效，并按百分号编码解码；`credentials` 优先于它。
它们不会进入生成的 AsyncAPI 文档，文档只报告服务连接的主机和端口。

`mqtts://` 选择 TLS，URL 没有指定端口时使用 8883。
[`tls_ca`](api:ruststream_rumqttc::MqttBroker::tls_ca) 是 PEM 格式的证书颁发机构，客户端用它验证服务器。
客户端只信任这个颁发机构，不使用任何其他证书库，所以没有 `tls_ca` 的 `mqtts://` URL 会让启动失败，错误信息会指出 `tls_ca`。
[`tls_client_auth`](api:ruststream_rumqttc::MqttBroker::tls_client_auth) 加入客户端的证书和密钥，用于按证书认证客户端的服务。

## 每个进程一个客户端 ID {/* #one-client-id-per-process */}

客户端 ID 是 `MqttBroker::new` 的第二个参数，服务器按它索引会话。
使用同一个 ID 的第二个连接会接管会话，并关闭第一个连接。
因此，服务的两个实例绝不共用一个 ID：这里每个实例从 `MQTT_CLIENT_ID` 取得自己的 ID，例如 pod 的名字。
需要分摊工作的实例改为加入[共享组](topics.md)。

## 限制 {/* #limits */}

- [`keep_alive`](../components/broker/limits.md) 是客户端在空闲连接上证明自己存活的频率；连接静默达到该间隔的一倍半时，服务器关闭连接。`rumqttc` 客户端的最小值是 5 秒，默认值是 60 秒。
- [`max_packet_size`](api:ruststream_rumqttc::MqttBroker::max_packet_size) 是服务接受的最大报文；服务器不会向它发送更大的报文。Broker 的默认值是 1 MiB，高于客户端自己的默认值 10 KiB。
- [`receive_maximum`](api:ruststream_rumqttc::MqttBroker::receive_maximum) 限制传输中的 `QoS` 1 和 2 投递数量（[投递保证](delivery.md)）。

## 服务 {/* #the-service */}

<Tabs groupId="api">
<TabItem value="macros" label="宏">

```rust title="src/main.rs" reference="examples/guide/production.rs#service"
```

</TabItem>
<TabItem value="manual" label="手写">

```rust title="src/main.rs" reference="examples/manual/guide/production.rs#service"
```

</TabItem>
</Tabs>

服务启动后，Broker 才建立连接。
连接丢失后会[重连](../components/broker/reconnect.md)，退避时间从 100 ms 开始，每次加倍，最长 5 秒；服务器报告会话已不存在时，Broker 重新订阅。
重试也无法消除的拒绝则会结束连接任务：凭据错误、服务器不接受的客户端 ID，或者证书无法完成的 TLS 握手。
此后每个订阅都返回这个错误。

## 测试配置 {/* #testing-the-configuration */}

测试在 `TestApp::start` 下启动生产环境的 `service`，配上生产环境的 `broker` 和测试设置。
进程内服务器读取 Broker 的配置，像客户端那样拒绝不合规的配置，但不会去连接服务器：

- keep-alive 小于 5 秒会让启动失败，错误消息中带有客户端的最小值；
- 没有 `tls_ca` 的 `mqtts://` URL 会让启动失败，错误信息会指出 `tls_ca`。

凭据和 TLS 握手只能由真实的服务器检查。
[安全](../components/broker/security.md)页面的线上测试在要求这两者的 Mosquitto 监听端口上运行它们。
