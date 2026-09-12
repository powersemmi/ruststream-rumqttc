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
| `Subscribe` | 是 | `MqttTopic` 描述对一个主题过滤器的订阅。参见[订阅](#subscriptions)。 |
| 确认（`ack` / `nack`） | 部分 | `QoS` 1 和 2 通过协议结算。`QoS` 0 和 `nack(requeue = true)` 返回 `AckError::Unsupported`。参见[确认](#acknowledgement)。 |
| `BatchSubscriber` | 在客户端 | 一个 PUBLISH 报文只携带一条消息，因此批次由 crate 自己攒，攒到挂载点指定的大小。参见[批次](#batches)。 |
| `TransactionalPublisher` | 否 | MQTT 没有事务。 |
| `OwnedTransactions` | 否 | MQTT 没有事务。 |
| `RequestReply` | 否 | MQTT 5 有响应主题属性，crate 在两个方向上都把它映射到 `reply-to` 消息头；带关联的 `request(msg, timeout)` 调用没有实现。响应方就是一个普通处理器，它发布到 `ctx.headers().reply_to()`。参见[消息头](#headers)。 |
| `Partitioned` | 否 | MQTT 没有分区，也没有路由键；顺序是一条连接上按主题保证的。 |
| `Seekable` / `Positioned` | 否 | Broker 每个主题只存一条保留消息，外加持久会话里尚未确认的消息，再没有别的可以定位过去。 |
| `DescribeServer` | 是 | `MqttBroker` 报告客户端连接的主机和端口，以及 `mqtt` 协议，AsyncAPI 模式记录的就是这些。URL 里的凭据不会进去。 |

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

`MqttTopic` 描述一条订阅：一个主题过滤器、一个服务质量和一个可选的共享组。它直接写在
`#[subscriber(..)]` 里；`ruststream_rumqttc::prelude` 重导出框架自己的 prelude，再加上这个 crate
的表面，因此一个 glob 就够一个服务文件用：

```rust
--8<-- "crates/ruststream-rumqttc/examples/mqtt_service.rs:handler"
```

应用指定 Broker 并挂载处理器：

```rust
--8<-- "crates/ruststream-rumqttc/examples/mqtt_service.rs:app"
```

过滤器属于部署而不属于代码的处理器，改写成 `#[subscriber(MqttTopic)]`，在挂载点用 `.name(..)`
给出过滤器；这时服务质量和共享组都取默认值。

订阅者被丢弃时，它的过滤器随之退订。

### 通配符 { #wildcards }

通配符就是协议自己的那两个：`+` 精确匹配一个主题层级，`#` 匹配主题的其余部分，并且只能出现在
最后一级。`MqttMessage::topic` 报告消息到达的那个具体主题，绝不是匹配上它的过滤器，因此挂在
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
过滤器：`filter()` 和投递时报告的主题仍是不带组名的形式。

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
`QoS` 0 上没有可以重投的东西，消息就没了。这里的 `retry()` 要读作“留到下一个会话”，而不是
“过一会儿再试”。

`HandlerOutcome::retry_after(delay)` 是在会话之内重试的那个结果，走的是框架自己的兜底路径而
不是协议：用 `retry_via(..)` 给挂载点一个重试发布者，运行时就会确认原件、等待，然后把一份副本
重新发布到同一个主题，消息头里带上重试次数。确认原件是这条兜底路径的第一步，它需要一次可以
确认的投递：在 `QoS` 0 上这一步被拒绝，延迟副本永远不会发布，消息因此丢失。没有配置重试发布者
时，运行时发出警告并退回 `retry()`，后果如上。

那份副本需要一个主题，而订阅只有在自己的过滤器就是一个主题时才说得出来。对
`devices/dev42/telemetry` 的订阅，向那里发布就能到达，共享组也一样 - 组会把那份副本分给成员。
对通配符过滤器的订阅根本无法这样到达，因为 `+` 和 `#` 只在订阅侧有效，因此 crate 直说自己给不出
地址，而不是给出一个谁也到不了的地址。于是，在通配符订阅之上设了 `retry_via(..)` 的作用域起不来，
并点名那条订阅：服务在启动时就知道 `retry_after` 在这里没有兜底，而不是让每一条延迟消息都丢在
一次发往虚空的发布里。

`HandlerOutcome::drop()` 是确认，因为丢弃是协议唯一的终态答复。发往 dead-letter 是服务自己做的
一次发布，不是 Broker 做的事。

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

确实指定了策略的挂载点写 `.out(marker, policy)`：处理器的返回值写
`.out(Reply, Publish::default().qos(Qos::ExactlyOnce))`，函数体持有的发布者则在 `Out` 槽位自己的
标记下写同一个调用。两处的参数都由策略给出，因此槽位和回复写法一致。

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
也不带：[单条消息的参数](#per-message-arguments)不是消息头，而是 PUBLISH 报文自己的协议字段，
因此不影响这条规则。

响应方就是一个普通处理器：进来的请求把响应主题放在 `reply-to` 消息头里，处理器读
`ctx.headers().reply_to()`，通过注入进来的发布者把答复发布到那个主题。

## 测试 { #testing }

`testing` feature 提供 `MqttTestBroker`，一个不需要服务器、不需要网络就能运行服务的进程内
Broker。从 `ruststream_rumqttc::testing` 导入它：路由文件导入的 prelude 是挂载点的词汇表，里面
没有它。测试把应用挂在这个 Broker 上，通过框架的 `TestApp` 测试套件驱动真实的处理器、编解码器
和中间件。参见
[用 TestApp 对服务做单元测试](https://powersemmi.github.io/ruststream/latest/guides/testing/#unit-testing-a-service-with-testapp)。

它攒批次的方式和真实订阅者一样，大小同样来自挂载点，期限也相同，因此批量处理器在测试套件下
收到的，就是服务器本会给出的东西。

路由文件原封不动地挂上去，两半都是。`MqttTopic` 在测试 Broker 上同样能建立订阅，因此服务交付的
那个处理器，就是测试套件挂载的那个处理器 - 本页开头那一个，连同通配符、服务质量和共享组 -
`MqttPublish` 也在它之上实例化发布者，因此 `b.include(handle).out(Reply, Publish::default())`
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
会话。策略上的 `retain` 出于同样的原因止步于实例化 - 进程内没有地方为每个主题保存最后一条消息。
因此，这个传输上的测试说的是处理器收到了什么、怎么结算的、把什么发布到了哪里；那些答案在协议上
同样成立，则由 `MQTT_TEST_URL` 开关控制的、针对 Eclipse Mosquitto 的真实测试来说明。持久会话上
的消息重放就是其中之一：一个断开又用同一个客户端 id 回来的订阅者，会收到它离开期间发布到它主题
上的消息，而这只有对着服务器跑才能证明。

框架的契约测试套件在两者上都跑。路由套件只在进程内跑，生命周期的那组转换和批量能力套件跑两遍，
一遍对着替身，一遍对着 Mosquitto。`tests/stand_in_mqtt.rs` 里的每个场景，都是
`tests/integration_mqtt.rs` 里某个真实场景的孪生，因此进程内断言的行为，都能追到支撑它的那次
服务器运行。

结算在这里给出的答案和协议上的一样，连拒绝都一样。`nack(requeue = true)` 在进程内报告
`AckError::Unsupported`，和真实消息一模一样，因为 MQTT 没有否定确认：返回
`HandlerOutcome::retry()` 的处理器在测试套件下不会得到重新投递，这个传输上的任何测试都无法声称
一次服务永远收不到的重试。[处理器的结果在这里做什么](#what-a-handlers-outcome-does-here)一节讲
的就是全部，进程内和对着服务器都一样。
