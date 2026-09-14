# MQTT { #mqtt }

`ruststream-rumqttc` 让 RustStream 服务运行在 MQTT 5 上，底层走
[`rumqttc`](https://docs.rs/rumqttc)。MQTT 是一条没有历史的主题总线。这个 crate 覆盖带通配符的
主题过滤器、服务质量、共享订阅、保留消息、会话和遗嘱消息，并在 `testing` feature 下提供一个
进程内 Broker 供测试使用。框架概念（怎么写订阅者、路由、编解码器、中间件）参见
[RustStream 文档](https://powersemmi.github.io/ruststream/)。

```toml
ruststream = { version = "0.7", features = ["macros"] }
ruststream-rumqttc = "0.7"
serde = { version = "1", features = ["derive"] }
```

## 能力 { #capabilities }

这个 Broker 原生实现了框架的哪些可选能力，以及确认在这里如何生效：

| 能力 | 原生 | 原因 |
| --- | --- | --- |
| `Subscribe` | 是 | `MqttTopic` 描述对一个主题的订阅，`MqttFilter` 描述对一个主题过滤器的订阅。参见[订阅](#subscriptions)。 |
| 确认（`ack` / `nack`） | 部分 | `QoS` 1 和 2 通过协议结算。`QoS` 0 和 `nack(requeue = true)` 返回 `AckError::Unsupported`。参见[确认](#acknowledgement)。 |
| `BatchSubscriber` | 在客户端 | 一个 PUBLISH 报文只携带一条消息，因此批次由 crate 自己攒，攒到挂载点指定的大小。参见[批次](#batches)。 |
| `TransactionalPublisher` | 否 | MQTT 没有事务。 |
| `OwnedTransactions` | 否 | MQTT 没有事务。 |
| `RequestReply` | 否 | MQTT 5 有响应主题属性，crate 在两个方向上都把它映射到 `reply-to` 消息头；带关联的 `request(msg, timeout)` 调用没有实现。响应方就是一个普通处理器，它发布到 `ctx.headers().reply_to()`。参见[消息头](#headers)。 |
| `Partitioned` | 否 | MQTT 没有分区，也没有路由键；顺序是一条连接上按主题保证的。 |
| `Seekable` / `Positioned` | 否 | Broker 每个主题只存一条保留消息，外加持久会话里尚未确认的消息，再没有别的可以定位过去。 |
| `DescribeServer` | 是 | `MqttBroker` 报告客户端连接的主机和端口、协议版本，以及它打开的会话。凭据不会进去。参见[生成的文档](#the-generated-document)。 |

## 生命周期 { #the-lifecycle }

Broker 的每个状态都是独立的类型：

```text
MqttBroker::new(url, client_id)   只有配置，同步，没有 I/O
  .connect()   ->  ConnectedMqttBroker   活动会话；订阅和发布者
  .shutdown()             ->             一次干净的 DISCONNECT，结束连接任务
```

`connect` 启动连接任务，在 Broker 的第一个 `CONNACK` 到达时返回；Broker 若改为发回拒绝，它就把
那个拒绝返回给你。

`shutdown` 消费已连接的 Broker，因此在它之后发布或订阅无法编译。先前发出的发布者比连接活得久，
连接消失之后它返回 `MqttError::NotConnected`，而不是在一个已经关闭的会话上照样报告成功。

会话和传输的设置都在同步构建器上。`credentials`、`keep_alive`、`clean_start` 和 `session_expiry`
配置会话，`last_will` 指定这个会话意外中断时 Broker 发布的消息。`max_packet_size` 默认 1 MiB，
高于客户端自己 10 KiB 的上限；`receive_maximum` 设置流量控制。`tls_ca` 和 `tls_client_auth`
用于要求客户端证书的托管 MQTT 服务。

## 订阅 { #subscriptions }

MQTT 有两个名字，这里就有两个描述符。`MqttTopic` 订阅一个主题；`MqttFilter` 订阅一个主题过滤器，
通配符也在内。两者都带一个服务质量和一个可选的共享组，都直接写在 `#[subscriber(..)]` 里，也都配
`ruststream_rumqttc::prelude`：它重导出框架自己的 prelude，再加上这个 crate 的表面，因此一个
glob 就够一个服务文件用：

```rust
--8<-- "crates/ruststream-rumqttc/examples/mqtt_service.rs:handler"
```

应用指定 Broker 并挂载处理器：

```rust
--8<-- "crates/ruststream-rumqttc/examples/mqtt_service.rs:app"
```

过滤器属于部署而不属于代码的处理器，改写成 `#[subscriber(MqttFilter)]`，在挂载点用 `.name(..)`
给出过滤器；这时服务质量和共享组都取默认值。订阅单个主题时 `#[subscriber(MqttTopic)]` 是一样的
写法。

除了接受哪些通配符，选哪个描述符还决定一件事：延迟的重新投递发布到哪里。主题是发布者能用的名字，
所以 `MqttTopic` 自己说得出副本从哪里回到这条订阅；过滤器不是这样的名字，所以 `MqttFilter` 上的
注册在挂载点点名那个主题。[处理器的结果在这里做什么](#what-a-handlers-outcome-does-here)讲的就是
这件事。

订阅者被丢弃时，它的过滤器随之退订。

### 通配符 { #wildcards }

通配符就是协议自己的那两个：`+` 精确匹配一个主题层级，`#` 匹配主题的其余部分，并且只能出现在
最后一级。它们属于 `MqttFilter`；把通配符交给 `MqttTopic` 会返回错误，点名该用哪个描述符，发生在
任何 I/O 之前。`MqttMessage::topic` 报告消息到达的那个具体主题，绝不是匹配上它的过滤器，因此挂在
`devices/+/telemetry` 上的处理器能读出读数是哪台设备发来的。通配符只在订阅侧有效：向含通配符的
主题发布会返回错误，什么也不发送。

无效的过滤器返回错误并点名该过滤器，发生在任何 I/O 之前。

### 服务质量 { #quality-of-service }

`Qos` 选择投递保证，默认是 `Qos::AtLeastOnce`：

| 取值 | 协议上的行为 |
| --- | --- |
| `Qos::AtMostOnce` | 发完就忘。这类投递不存在确认。 |
| `Qos::AtLeastOnce` | 投递用 `PUBACK` 确认。 |
| `Qos::ExactlyOnce` | 四个报文的握手；第二段由客户端完成。 |

### 共享订阅 { #shared-subscriptions }

`MqttTopic::new("jobs").shared("workers")` 订阅的是 `$share/workers/jobs`。Broker 把匹配的消息
分给组内成员，而不是给每人一份副本，MQTT 就是这样表达竞争消费者的。组名只属于订阅用的那个
过滤器：`topic()`、`filter()` 和投递时报告的主题仍是不带组名的形式。

同一条连接上一个组的两个成员，在 Broker 看来是一条订阅，因此它们的投递由 crate 轮流分发。共享
组名为空，或者含有 `/`、`+`、`#`，都会在任何 I/O 之前返回错误，和无效过滤器一样。

### 批次 { #batches }

批量处理器接收 `&[T]`，大小由挂载点指定：

```rust
--8<-- "crates/ruststream-rumqttc/examples/mqtt_batches.rs:batches"
```

一个 PUBLISH 报文只携带一条消息，因此批次由 crate 自己攒。批次在攒满挂载点指定的大小时关闭，
或者在第一条投递之后 20 毫秒关闭，以先到者为准。大小属于挂载点，期限属于 crate，而一个批次
装的绝不会超过它开启时的大小。空闲的订阅会一直等第一条投递，因此安静的主题不花任何代价。

挂载点上没有任何东西说明批次是 crate 攒的还是 Broker 给的，因此为别的 Broker 写的批量处理器
原封不动挂在这里。确认仍然是逐条投递的：攒到一半被放弃的批次，里面的消息就留在未确认状态，
等持久会话恢复时重新投递。

## 确认 { #acknowledgement }

确认跟随投递自身的服务质量；消息在处理器返回时结算，而不是在它到达时结算：

- `QoS` 1 和 2 通过协议确认。
- `QoS` 0 的投递返回 `AckError::Unsupported`：协议里没有给它们准备确认报文。
- `nack(requeue = true)` 同样返回 `AckError::Unsupported`。MQTT 没有否定确认，未确认的消息在
  持久会话恢复时重新投递。
- `nack(requeue = false)` 是确认：丢弃是协议提供的唯一终态结果。

两个互相重叠的过滤器同时匹配一条消息时，确认只属于其中一次投递，其余副本返回
`AckError::Unsupported`。

### 处理器的结果在这里做什么 { #what-a-handlers-outcome-does-here }

`HandlerOutcome::retry()` 是请 Broker 重新投递，而在 MQTT 上没有任何东西可以去请。运行时把这次
被拒绝的否定确认记进日志（`ack / nack failed`）然后继续，这次投递因此始终没有被确认。所以在
`QoS` 1 和 2 上，消息会在持久会话恢复时回来 - `clean_start(false)`、长到能熬过中断的会话过期
时间，再加一次重连 - 而绝不会在当前这条连接里回来：处理器返回后的那几秒内什么也不会重试。在
`QoS` 0 上没有可以重新投递的东西，消息就没了。这里的 `retry()` 要读作“留到下一个会话”，而不是
“过一会儿再试”。

`HandlerOutcome::retry_after(delay)` 是在会话之内重试的那个结果，走的是框架自己的兜底路径而
不是协议。运行时确认原件、等待，然后发布一份带着重试次数的副本。确认原件是这条兜底路径的第一步，
它需要一次可以确认的投递：在 `QoS` 0 上这一步被拒绝，延迟副本永远不会发布，消息因此丢失。

一直要求重试的处理器会让自己的消息一直转下去，直到有人来干预；`include` 之后紧跟的两步结束这件
事：

```rust
--8<-- "crates/ruststream-rumqttc/examples/mqtt_retries.rs:declaration"
```

`max_attempts(n)` 是一条消息一共得到几次投递，第一次也算在内。MQTT 自己不数重新投递，所以这个数
记在框架的重试次数消息头里，随副本一起走。`dead_letter(topic)` 是次数用完之后消息去的主题；给它
一个本服务的订阅都不读的主题，因为与某条活着的过滤器匹配的主题会把消息直接还回来。只声明上限而
不给主题，则是拒绝这条消息，而在 MQTT 上这意味着确认它然后放手。

副本发布到哪里，要么由描述符回答，要么由挂载点回答。`MqttTopic` 订阅一个主题，所以它自己说得出
副本从哪里回到这条订阅 - 共享订阅也一样，组会把那份副本分给成员 - 上面那段声明就是挂载点的全部。
`MqttFilter` 订阅许多主题，一个也点不出来，因为 `+` 和 `#` 只在订阅侧有效，于是由注册来点名：

```rust
--8<-- "crates/ruststream-rumqttc/examples/mqtt_retries.rs:named"
```

与过滤器匹配的主题把副本送回同一条订阅。在过滤器上的注册既不点名主题也不挂发布变换，就起不来，
并点名那条订阅：服务在启动时就知道 `retry_after` 在这里无处可去，而不是让每一条延迟消息都丢在
一次发往虚空的发布里。

另一种做法是每次投递各点各的名。过滤器读许多主题，而每条消息只属于其中一个，所以把副本送回它自己
那次投递到达的主题，它就回到那台设备，而不是回到为整个设备群选定的一个主题。这个主题在这个 Broker
的投递上下文里，键是 `DeliveryTopic`：

```rust
--8<-- "crates/ruststream-rumqttc/examples/mqtt_retries.rs:naming_transform"
```

发布变换读的是这个上下文，所以处理器也要把它写出来：像上面那样用 `Ctx<DeliveryTopic>`，或者写一个
`ctx: &mut Context<'_, MqttContext>` 参数。挂载点这时挂上这个发布变换，而不是点名主题：

```rust
--8<-- "crates/ruststream-rumqttc/examples/mqtt_retries.rs:naming_mount"
```

两种做法互斥：一条注册要么点名主题，要么挂一个命名发布变换，两个都写编译不过。

`out_retry(policy)` 同时替换副本出去的那个发布者 - 否则用的是这个 Broker 的默认策略。这个位置
就是一个普通槽位，所以它后面接的是槽位的那几步：`.codec(..)`、`.transform(..)` 和
`.map_publisher(..)`。延迟副本带的是投递本身的字节，所以这里点名的编解码器只把位置解析出来，
并不编码任何东西，而发布变换会在副本上执行：服务只有在这里才能把一次重新投递标记成重新投递。
这里的发布变换读的是正在重试的那次投递，和回复上的发布变换一样。

`HandlerOutcome::drop()` 是确认，因为丢弃是协议唯一的终态答复。

背压就是协议的 receive-maximum，由 `MqttBroker::receive_maximum` 设置：Broker 同时在途的未确认
`QoS` 1/2 投递不超过这个数，未被读取的订阅者队列也由它兜住上界。`QoS` 0 没有这样的上界。

## 重连 { #reconnection }

crate 拥有一个任务，它轮询客户端唯一的事件循环，把每个报文交给过滤器与之匹配的那些订阅。
keep-alive、确认和流量控制都靠轮询推动，因此订阅和发布改由调用方的任务发出，慢消费者于是绝不会
拖住 keep-alive 流量。

这个任务自己重连，退避从 100 毫秒开始，翻倍直到 5 秒的上限。Broker 不会再改主意的拒绝（凭据
错误、客户端 id 不可接受）则会结束这个任务，之后每条订阅都返回那个错误。

重连之后订阅会怎样，由 `CONNACK` 里的 `session_present` 标志决定。会话还在时，过滤器仍然注册着，
什么也不会重发；会话没了时，每条活动订阅都会重新订阅一遍。

## 发布 { #publishing }

`MqttPublish` 是构造发布者的策略，它声明两样东西：服务质量和保留标志。它同时是这个 Broker 的
默认策略，因此 `#[subscriber(.., publish)]` 处理器在没有指定自己的策略时，就通过它发送。回复
发往它自己的类型所声明的主题。

确实指定了策略的挂载点，按位置分别点名：`.out_reply(policy)` 承载处理器的返回值，
`.out_retry(policy)` 承载延迟重试发出的那份副本，`.out(marker, policy)` 则承载函数体在自己的
槽位标记下持有的发布者。三处的参数都由策略给出，因此
`.out_reply(Publish::default().qos(Qos::ExactlyOnce))` 和它旁边的槽位写法一致。

一个文件写哪个名字，取决于它导入的 prelude。处理器文件导入 `ruststream::prelude::*`，用能力
trait 约束注入进来的发布者（`Out<impl Publisher>`），函数体因此完全不提 Broker 类型。路由文件
导入 `ruststream_rumqttc::prelude::*`，策略在那里的名字是 `Publish`：挂载点于是不管跑在哪个
Broker 上读起来都一样，移植服务改的是导入而不是调用。`MqttPublish` 留在 crate 根上，供混用两个
Broker、必须说清指的是哪一个的文件使用。

发布在消息归客户端会话所有时就返回，而不是等 Broker 确认之后。在 `QoS` 1 和 2 上，会话会一直
重传到 Broker 确认为止，跨重连也是如此。

MQTT 的负载常常是服务手上已经是字节的值（一个状态字符串、一帧传感器数据、一条 protobuf 记录），
而不是需要框架去编码的模型。声明为已序列化的类型原样发送这些字节，路径上没有编解码器，它的声明
照样指定主题。名字里的 `{placeholder}` 会变成一个由调用填入的 setter：

```rust
--8<-- "crates/ruststream-rumqttc/examples/mqtt_retained.rs:state"
```

### 单条消息的参数 { #per-message-arguments }

MQTT 在 PUBLISH 报文上携带的两个参数，都是发布上的步骤，先后顺序随意：

| 步骤 | 为这一条消息设置什么 |
| --- | --- |
| `qos(qos)` | 投递的服务质量。 |
| `retain(retain)` | Broker 是否把这条消息留作该主题的保留消息。 |

调用没有指定的参数，取挂载点策略声明的那个值。因此 `Publish::default().qos(Qos::ExactlyOnce)`
是这个发布者每次发布的默认值，而步骤是某一条消息与众不同的地方：

```rust
--8<-- "crates/ruststream-rumqttc/examples/mqtt_retained.rs:per_publish"
```

这些步骤落在发布本身，而不是落在另外一个发布者上，因此带步骤的发布在其余方面就是一次普通发布：
它用自己挂载点指定的编解码器编码，槽位发布仍然记在自己的槽位上，`tb.out::<Marker>()` 也和记录
其他发布一样记录它。凡是构建发布的地方都有这些步骤：处理器函数体里、启动钩子里、测试里。

这些值以协议字段的身份到达客户端，因此它们当中没有任何一项是作为用户属性发送的，订阅者看到的是
一条普通消息。没有自己调用点的发布 - 一次回复，或者运行时为延迟重试发布的那份副本 - 整个按策略
来。

调整其中一项的处理器函数体，是函数体唯一会点名这个 Broker 的地方。它导入
`ruststream_rumqttc::prelude::*` 取得这些步骤，并用 `MqttPublishOptions` 约束自己的槽位，签名
因此说明了这个函数体是为哪个 Broker 写的：

```rust
--8<-- "crates/ruststream-rumqttc/examples/mqtt_retained.rs:stepped_handler"
```

默认值由它的挂载点声明，而步骤是某一条消息改动的部分：

```rust
--8<-- "crates/ruststream-rumqttc/examples/mqtt_retained.rs:stepped_mount"
```

测试点名同一个类型来断言一次发布带了什么：
`tb.out::<States>().assert_called_once().with_options(&MqttPublishOptions::default().retain(true))`，
而 `assert_options_default()` 断言的是这次发布一个步骤也没有取。

### 保留消息 { #retained-messages }

`Publish::default().retain(true)` 发布保留消息：Broker 为每个主题保留最后一条消息，并把它交给
过滤器匹配上的每一个新订阅者。在设备发布了自己的状态之后才启动的服务，照样能收到那个状态。保留
消息不会到达共享订阅。

发送什么都保留的发布者，在自己的策略上声明一次这个标志；只发一次的通告则用 `retain(true)` 按
消息取它。两种写法下，作用域的 `after_startup` 钩子都在 Broker 连上之后执行这次发布：

```rust
--8<-- "crates/ruststream-rumqttc/examples/mqtt_retained.rs:retained"
```

## 消息头 { #headers }

消息头以 MQTT 5 用户属性发送，因此这个 crate 不发明信封格式，非 Rust 的对端看到的是普通的 MQTT
消息。众所周知的 `content-type`、`reply-to` 和 `correlation-id` 三个消息头改为占用各自对应的
一等属性（内容类型、响应主题、关联数据），两个方向都是如此。没有消息头的消息发布出去时一个属性
也不带：[单条消息的参数](#per-message-arguments)不是消息头，而是 PUBLISH 报文自己的字段，每一项
的取值都在报文组装之前、在策略的默认值之上定下来。

媒体类型还决定报文的一个属性。内容类型是文本的那种发布 - `application/json`、任何 `text/` 子类型、
任何以 `+json` 结尾的厂商类型 - 带的负载格式指示是 1，其余的是 0，于是非 Rust 的对端把 JSON 正文
按它本来的 UTF-8 读。`content-type` 消息头由框架按发布位置的编解码器填上，这就是这项指示跟着
编解码器走、却不需要声明任何东西的原因。

响应方就是一个普通处理器：进来的请求把响应主题放在 `reply-to` 消息头里，处理器读
`ctx.headers().reply_to()`，通过注入进来的发布者把答复发布到那个主题。

## 生成的文档 { #the-generated-document }

框架从服务自己的声明生成 AsyncAPI 文档，而这个 crate 填上只有 MQTT 才知道的那部分。打开
`asyncapi` 能力即可，它转发框架的同名能力：

```toml
ruststream-rumqttc = { version = "0.7", features = ["asyncapi"] }
```

服务器声明自己说的是 MQTT 5，并描述客户端打开的那个会话：

```json
--8<-- "crates/ruststream-rumqttc/tests/bindings/server.json"
```

有两样东西是故意不写的。凭据不会进入一份团队发布并分享的文档，所以 URL 里的用户信息和
`credentials` 都不会出现。遗嘱消息交出主题、服务质量和保留标志，但不交出负载：那是消息的内容而
不是坐标，而且它可能是内部的东西。

订阅在自己的接收操作上报告读取时用的服务质量：

```json
--8<-- "crates/ruststream-rumqttc/tests/bindings/receive_operation.json"
```

发布策略报告自己报文上的那两个参数，写在 `Out` 槽位或 dead-letter 主题的发送操作上。回复没有
自己的发送操作，所以回复策略在那里什么也不添：

```json
--8<-- "crates/ruststream-rumqttc/tests/bindings/send_operation.json"
```

每条消息都报告 crate 为它映射的那几个 MQTT 5 属性，但负载格式指示不在其中：它跟着单条消息的媒体
类型走，而那是发布位置的编解码器产出的，描述符和策略都拿不到那个编解码器 - 文档报告媒体类型本身，
写在框架填上的 `contentType` 里：

```json
--8<-- "crates/ruststream-rumqttc/tests/bindings/message.json"
```

服务发布的消息只报告关联数据。响应主题是发起请求的一方在自己的请求上设置的属性，而回复不是
请求，所以这份绑定不描述它：

```json
--8<-- "crates/ruststream-rumqttc/tests/bindings/outgoing_message.json"
```

在请求自带的响应主题上作答的响应方没有固定的回复通道，所以文档把回复地址报告为 `null`，并把读者
指向 `$message.header#/reply-to` - 响应主题到达时所在的那个消息头。

## 测试 { #testing }

`testing` feature 提供 `MqttTestBroker`，一个不需要服务器、不需要网络就能运行服务的进程内
Broker。从 `ruststream_rumqttc::testing` 导入它：路由文件导入的 prelude 是挂载点的词汇表，里面
没有它。测试把应用挂在这个 Broker 上，通过框架的 `TestApp` 测试套件驱动真实的处理器、编解码器
和中间件。参见框架的
[`testing` 模块](https://docs.rs/ruststream/latest/ruststream/testing/index.html)。

它攒批次的方式和真实订阅者一样，大小同样来自挂载点，期限也相同，因此批量处理器在测试套件下
收到的，就是服务器本会给出的东西。

路由文件原封不动地挂上去，两半都是。`MqttTopic` 和 `MqttFilter` 在测试 Broker 上同样能建立
订阅，因此服务交付的
那个处理器，就是测试套件挂载的那个处理器 - 本页开头那一个，连同通配符、服务质量和共享组 -
`MqttPublish` 也在它之上实例化发布者，因此 `b.include(handle).out_reply(Publish::default())`
在两个 Broker 下是同一行。没有进程内的描述符，也没有进程内的策略要换进来；变的只有构建应用时
用的那个 Broker。

测试 Broker 按主题过滤器匹配来路由，用的正是连接任务分发投递时的那条规则，因此同一个过滤器在
进程内选中的主题，和它在协议上选中的一样，发布 `devices/dev42/telemetry` 的设备能到达函数体。
描述符在这里同样会被校验：服务器会拒绝的过滤器在这里就起不来，而不是先通过它的第一个测试。

描述符其余的部分，只要答案在没有服务器时还观察得到，就一律照做。共享组让成员之间互相竞争：四次
发布是整个组四次投递，而不是每个成员各一次，因此测试可以断言工作被分摊了，而不只是断言它发生了。
服务质量决定一次投递能不能结算 - 取发布和订阅两者中较小的那个，和协议上一样 - 因此 `QoS` 0 的
投递在这里报告 `AckError::Unsupported`，和它对着 Mosquitto 时一模一样，处理器也就无法悄悄证明
一个没人要求过的保证。

这个替身留在外面的是协议本身：可确认的 `QoS` 背后那次确认交换、保留消息，以及负责重新投递的
会话。`retain` 标志出于同样的原因解析完就止步：测试仍然断言得出这次发布要的是什么，但进程内没有
地方为每个主题保存最后一条消息。因此，这个传输上的测试说的是处理器收到了什么、怎么结算的、
把什么发布到了哪里；那些答案在协议上同样成立，则由 `MQTT_TEST_URL` 开关控制的、针对
Eclipse Mosquitto 的真实测试来说明。持久会话上的消息重放就是其中之一：一个断开又用同一个
客户端 id 回来的订阅者，会收到它离开期间发布到它主题上的消息，而这只有对着服务器跑才能证明。

框架的契约测试套件在两者上都跑。路由套件只在进程内跑，生命周期的那组转换和批量能力套件跑两遍，
一遍对着替身，一遍对着 Mosquitto。`tests/stand_in_mqtt.rs` 里的每个场景，都是
`tests/integration_mqtt.rs` 里某个真实场景的孪生，因此进程内断言的行为，都能追到支撑它的那次
服务器运行。

结算在这里给出的答案和协议上的一样，连拒绝都一样。`nack(requeue = true)` 在进程内报告
`AckError::Unsupported`，和真实消息一模一样，因为 MQTT 没有否定确认：返回
`HandlerOutcome::retry()` 的处理器在测试套件下不会得到重新投递，这个传输上的任何测试都无法声称
一次服务永远收不到的重试。[处理器的结果在这里做什么](#what-a-handlers-outcome-does-here)一节讲
的就是全部，进程内和对着服务器都一样。
