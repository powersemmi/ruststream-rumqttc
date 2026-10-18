---
title: Тестирование сервиса на MQTT
sidebar_position: 8
description: "Тестирование сервиса на MQTT внутри процесса с TestApp: рабочее приложение, темы с подстановочными знаками, опубликованные тревоги, задержки на остановленных часах и тот же тест на Mosquitto."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Тестирование сервиса на MQTT

Тест запускает собственное приложение сервиса через [`TestApp`](api:ruststream::testing::TestApp) на [внутрипроцессном сервере MQTT](../components/testing/index.md) или на Mosquitto.

Тестируемый сервис следит за морозильными камерами.
Каждая камера публикует свою температуру в `freezers/<id>/temperature`, а мониторы делят показания в группе совместной подписки.
Каждое показание записывается в журнал температур.
Камера теплее -18 °C поднимает тревогу в `freezers/alerts`.
Когда журнал температур не отвечает, обработчик через [`retry_after`](../components/settlement/retry-after.md) просит доставить показание снова через десять секунд.
После трёх доставок показание уходит в `freezers/review`, [тему dead-letter](../components/settlement/dead-letters.md).

Обработчик `watch` публикует тревогу через слот `Alerts`.
Приложение строит `service(broker)`, поэтому тест выбирает брокер сам, а `main` берёт рабочий.
Тестам нужна фича `testing` крейта `ruststream-rumqttc` в `[dev-dependencies]`.

## Тёплая камера

<Tabs groupId="api">
<TabItem value="macros" label="Макросы">

```rust title="src/main.rs" reference="examples/guide/testing.rs#in_process"
```

</TabItem>
<TabItem value="manual" label="Вручную">

```rust title="src/main.rs" reference="examples/manual/guide/testing.rs#in_process"
```

</TabItem>
</Tabs>

[`TestApp::start`](api:ruststream::testing::TestApp::start) подключает [`MqttBroker`](../components/broker/index.md) к внутрипроцессному серверу MQTT.
Соединение по URL не устанавливается.
Тест обращается к брокеру по его рабочему типу: `tb.broker::<MqttBroker>()`.
`Reading` не называет темы, поэтому тест называет её сам через `.to("freezers/f1/temperature")`, как это сделала бы камера.
Фильтр обработчика сопоставляет её так же, как Mosquitto.
`publish` возвращает управление, когда обработчик и его тревога завершены.
Поэтому следующие строки проверяют уже готовое состояние: как подписка завершила доставку и что дошло до `freezers/alerts`.
К подписке тест обращается по её фильтру, без группы совместной подписки.
Тело теста принимает запущенное приложение аргументом, поэтому его же выполняет и тест на Mosquitto ниже.

## Холодная камера

<Tabs groupId="api">
<TabItem value="macros" label="Макросы">

```rust title="src/main.rs" reference="examples/guide/testing.rs#cold"
```

</TabItem>
<TabItem value="manual" label="Вручную">

```rust title="src/main.rs" reference="examples/manual/guide/testing.rs#cold"
```

</TabItem>
</Tabs>

`settled` проверяет исход, который вернул обработчик.
[`tb.out::<Alerts>()`](api:ruststream::testing::TestApp::out) читает то, что обработчик опубликовал через слот `Alerts`, а `assert_not_called` проверяет, что он ничего не опубликовал.

## Задержка без ожидания

<Tabs groupId="api">
<TabItem value="macros" label="Макросы">

```rust title="src/main.rs" reference="examples/guide/testing.rs#delayed"
```

</TabItem>
<TabItem value="manual" label="Вручную">

```rust title="src/main.rs" reference="examples/manual/guide/testing.rs#delayed"
```

</TabItem>
</Tabs>

Тест начинается с остановленными часами, а [`advance`](api:ruststream::testing::TestApp::advance) переводит их вперёд.
Десять секунд `RETRY` проходят мгновенно.
Когда задержка истекает, `retry_after` публикует копию показания в `freezers/retry/temperature`, и фильтр читает её снова.
В копии есть заголовок с числом попыток, и `published` его показывает.
Третья доставка - последняя, которую разрешает точка монтирования, поэтому показание уходит в `freezers/review` в том виде, в каком пришло.
Каждая отложенная доставка записывается как `Outcome::Nack`.

## Что моделирует внутрипроцессный сервер

Внутрипроцессный сервер читает собственные настройки брокера, а публикация проходит ту же проверку и то же отображение на свойства MQTT 5, что и при передаче по сети.
Он моделирует сервер, с которым работает одно соединение:

- Публикация доходит до каждой подписки, фильтр которой подходит к её теме, с учётом `+`, `#` и правила для тем, которые начинаются с `$`.
- Группа совместной подписки получает одну копию на всех участников, а пересекающиеся фильтры получают каждый свою.
- Сохранённое сообщение доходит до более поздней подписки вне группы совместной подписки, а пустая сохранённая полезная нагрузка его стирает.
- Сообщение больше `max_packet_size` не доходит, а тема или заголовок, которые отклоняет клиент, приводят к ошибке публикации.
- Подтверждение после остановки отклоняется.

Каждый брокер, подключённый внутри процесса, - это отдельный сервер.
Постоянной сессии и её возобновлению, последней воле, окну Receive Maximum, обменам подтверждениями при `QoS` 1 и 2 и нескольким соединениям с одним сервером нужен настоящий брокер.

## Тот же тест на Mosquitto

<Tabs groupId="api">
<TabItem value="macros" label="Макросы">

```rust title="src/main.rs" reference="examples/guide/testing.rs#live"
```

</TabItem>
<TabItem value="manual" label="Вручную">

```rust title="src/main.rs" reference="examples/manual/guide/testing.rs#live"
```

</TabItem>
</Tabs>

[`TestApp::start_live`](../components/testing/live.md) подключает то же приложение к работающему серверу.
Тест читает адрес из `MQTT_TEST_URL` через `live::url`, собственную вспомогательную функцию примера, и сразу завершается, если переменная не задана.
Он подключается со своим идентификатором клиента, поэтому делит сервер с другими запусками.
Проверки те же, что внутри процесса, без изменений.

```bash
cargo test
MQTT_TEST_URL=mqtt://localhost:1883 cargo test -- --test-threads=1
```

`RUSTSTREAM_REQUIRE_LIVE=1` превращает пропуск в ошибку. Это нужно задаче CI, которая запускает сервер и не должна проходить без него.
