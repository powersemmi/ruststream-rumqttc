---
title: Начало работы
sidebar_position: 1
description: "Установка ruststream-rumqttc, локальный Mosquitto, первый подписчик на нём и его тест без сервера."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Начало работы

Это руководство устанавливает `ruststream-rumqttc`, запускает локальный сервер MQTT и первый сервис на нём.

## Установка

```toml title="Cargo.toml"
[dependencies]
ruststream = { version = "0.7", features = ["macros", "json", "logging"] }
ruststream-rumqttc = "0.7"
serde = { version = "1", features = ["derive"] }
tracing = "0.1"

[dev-dependencies]
ruststream-rumqttc = { version = "0.7", features = ["testing"] }
tokio = { version = "1", features = ["macros", "rt"] }
```

`ruststream` - это сам фреймворк. Фича `macros` даёт атрибуты, `json` - кодек, а `logging` выводит журнал сервиса в терминал.
`ruststream-rumqttc` - брокер MQTT 5.
Собственного кодека брокер не подключает, поэтому сервис выбирает `json`, `msgpack` или `cbor` в фичах `ruststream`.

У крейта брокера две фичи, обе выключены по умолчанию:

| Фича | Что она даёт |
|---|---|
| `testing` | [внутрипроцессный сервер MQTT](../components/testing/index.md), на котором работает `TestApp` |
| `asyncapi` | [привязки MQTT](../components/asyncapi/index.md) в генерируемом документе AsyncAPI |

## Системные требования

Клиент `rumqttc` работает с TLS через rustls, а криптографию rustls берёт из `aws-lc-rs`.
`aws-lc-rs` компилирует код на C, поэтому для сборки нужен компилятор C.
В Debian и Ubuntu его ставит пакет `build-essential`.
Требования для других платформ перечислены в [документации aws-lc-rs](https://aws.github.io/aws-lc-rs/requirements/index.html).

## Локальный сервер MQTT

Для разработки достаточно одного контейнера Eclipse Mosquitto.
Свои слушатели он читает из файла конфигурации:

```text title="mosquitto.conf"
listener 1883
allow_anonymous true
```

```yaml title="compose.yaml"
services:
  mosquitto:
    image: eclipse-mosquitto:2
    ports:
      - "1883:1883"
    volumes:
      - ./mosquitto.conf:/mosquitto/config/mosquitto.conf:ro
```

```bash
docker compose up -d
```

Сервер слушает `localhost:1883` и принимает клиентов без пароля.

## Первый сервис

<Tabs groupId="api">
<TabItem value="macros" label="Макросы">

```rust title="src/main.rs" reference="examples/guide/getting_started.rs#service"
```

</TabItem>
<TabItem value="manual" label="Вручную">

```rust title="src/main.rs" reference="examples/manual/guide/getting_started.rs#service"
```

</TabItem>
</Tabs>

`record` читает телеметрию каждого устройства в JSON.
Показание он подтверждает, а показание ниже абсолютного нуля отбрасывает.
[`MqttFilter`](../components/subscriptions/mqtt-filter.md) подписывается на фильтр темы: `+` заменяет один уровень темы, здесь идентификатор устройства.
[`Qos::AtLeastOnce`](../components/subscriptions/qos.md) делает доставку каждого показания подтверждаемой: подтверждение уходит серверу, когда `record` возвращает управление.
Фильтру соответствует много тем, поэтому тему для [отложенной переобработки](../components/settlement/out-retry.md) называет точка монтирования.
Её называет каждая подписка на фильтр, а без неё сервис не запускается.

[`MqttBroker::new`](../components/broker/index.md) принимает URL сервера и идентификатор клиента и только запоминает их.
Соединение открывается при запуске сервиса.
По идентификатору клиента сервер узнаёт сессию сервиса, поэтому у каждого запущенного процесса он свой.
`#[ruststream::app]` превращает `app` в `main`. В ручной форме то же делает [`run_main`](api:ruststream::runtime::cli::run_main).

Запустите сервис:

```bash
cargo run -- run
```

Опубликуйте показание из другого терминала клиентом, который входит в образ:

```bash
docker compose exec mosquitto mosquitto_pub -V mqttv5 -q 1 \
  -t devices/dev42/telemetry -m '{"device":"dev42","temperature":21.5}'
```

Сервис пишет в журнал `reading recorded` с устройством и его температурой.
<kbd>Ctrl</kbd>+<kbd>C</kbd> останавливает его.

## Первый тест

В файле примера есть и два теста.
Каждый запускает то же `app` через `TestApp::start`, публикует показание в `devices/dev42/telemetry` и проверяет, как `record` завершил его доставку.
Там `MqttBroker` подключается к внутрипроцессному серверу MQTT, поэтому тестам не нужен Mosquitto:

```bash
cargo test
```

Руководство [Тестирование сервиса на MQTT](testing.md) разбирает эти тесты и то, что моделирует внутрипроцессный сервер.

## Переход с rumqttc

Сервис на rumqttc сам опрашивает цикл событий и вручную подтверждает каждое сообщение.
Вот куда переходит каждая часть в RustStream:

| rumqttc | RustStream |
|---|---|
| `MqttOptions` и его сеттеры | билдер [`MqttBroker`](../components/broker/index.md): [`keep_alive`](../components/broker/limits.md), [`credentials`](../components/broker/security.md), [`clean_start`](../components/broker/session.md), [`last_will`](../components/broker/last-will.md) |
| цикл `EventLoop::poll` | собственная задача соединения брокера, которая переподключается и подписывается заново |
| `AsyncClient::subscribe` | обработчик `#[subscriber]` на [`MqttTopic`](../components/subscriptions/mqtt-topic.md) или [`MqttFilter`](../components/subscriptions/mqtt-filter.md) |
| десериализация `publish.payload` | тип параметра обработчика, который декодирует кодек |
| `set_manual_acks(true)` и `AsyncClient::ack` | исход обработчика ([Гарантии доставки](delivery.md)) |
| `AsyncClient::publish` | ответ или параметр `Out`, отправленный через [политику публикации](../components/publishing/index.md) ([Публикация команд](publishing.md)) |
