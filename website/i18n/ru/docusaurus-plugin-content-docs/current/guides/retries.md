---
title: Переобработка и темы dead-letter
sidebar_position: 6
description: "Переобработка сообщения MQTT сразу или позже, предел попыток, тема dead-letter для того, что раз за разом не обрабатывается, и возврат копий фильтра в их собственные темы."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Переобработка и темы dead-letter

Это руководство обрабатывает сообщение повторно сразу или позже, ограничивает число попыток и переносит то, что раз за разом не обрабатывается, в [тему dead-letter](../components/settlement/dead-letters.md).

MQTT не придерживает сообщения, не считает доставки, и очереди dead-letter в нём нет.
Всё это делает фреймворк: точка монтирования объявляет предел попыток и тему dead-letter, а для переобработки публикуется копия сообщения с числом попыток в заголовке.

Пример - сервис прошивок.
Он устанавливает обновления на устройства, а устройство может какое-то время отказываться от обновления.
Ещё он сохраняет отчёты, которые присылает каждое устройство.

## Подписчик

Устройство отказывается от обновления тремя способами, и каждому нужен свой исход.
`Busy` значит, что устройство выполняет задание. Обновление, предложенное прямо сейчас, снова получит отказ, поэтому [`retry_after`](../components/settlement/retry-after.md) предлагает его через 30 секунд.
`Interrupted` значит, что передача оборвалась: [`retry`](../components/settlement/retry.md) пробует снова сразу.
`Incompatible` значит, что образ не подходит устройству. Новая попытка получит тот же ответ, поэтому обновление отбрасывается.

<Tabs groupId="api">
<TabItem value="macros" label="Макросы">

```rust title="src/main.rs" reference="examples/guide/retries.rs#install"
```

</TabItem>
<TabItem value="manual" label="Вручную">

```rust title="src/main.rs" reference="examples/manual/guide/retries.rs#install"
```

</TabItem>
</Tabs>

`retry_after` подтверждает доставку, ждёт и затем публикует копию обратно в `updates/install`.
В копии есть заголовок с числом попыток ([`RETRY_COUNT_HEADER`](api:ruststream::runtime::RETRY_COUNT_HEADER)), увеличенным на единицу.
Это работает при любом качестве обслуживания: у доставки с `QoS` 0 подтверждать нечего, а копия всё равно публикуется.
Копия ждёт в памяти сервиса: процесс, который остановился до конца задержки, её теряет.

`retry` тоже становится копией, которая публикуется сразу, потому что точка монтирования ниже объявляет предел попыток.
MQTT не считает повторные доставки, поэтому предел считает копии по их заголовку.
Без предела `retry` оставляет доставку неподтверждённой до следующей сессии, как показывает руководство [Гарантии доставки](delivery.md).

## Предел попыток и тема dead-letter

<Tabs groupId="api">
<TabItem value="macros" label="Макросы">

```rust title="src/main.rs" reference="examples/guide/retries.rs#app"
```

</TabItem>
<TabItem value="manual" label="Вручную">

```rust title="src/main.rs" reference="examples/manual/guide/retries.rs#app"
```

</TabItem>
</Tabs>

[`max_attempts`](../components/settlement/retries.md) считает все доставки одного обновления, включая первую.
Четвёртая доставка не обрабатывается повторно: [`dead_letter`](api:ruststream::runtime::RouterWith::dead_letter) публикует её в `updates/dead` с полезной нагрузкой и заголовками в том виде, в каком они пришли.
Отброшенное обновление завершается и никуда не уходит. В теме dead-letter лежат только обновления, у которых кончились попытки.

Копия - обычная публикация, поэтому её получает каждая подписка, фильтр которой подходит к её теме, в этом сервисе и в любом другом.
Поэтому ни тема переобработки, ни тема dead-letter не подходит ни к одному другому фильтру, который читает сервис.
Подписка на `updates/#` рядом с `install` читала бы каждую копию каждого обновления и заодно каждое сообщение dead-letter.

## Чтение темы dead-letter

<Tabs groupId="api">
<TabItem value="macros" label="Макросы">

```rust title="src/main.rs" reference="examples/guide/retries.rs#review"
```

</TabItem>
<TabItem value="manual" label="Вручную">

```rust title="src/main.rs" reference="examples/manual/guide/retries.rs#review"
```

</TabItem>
</Tabs>

Тема dead-letter - обычная тема, и `review` читает её, как любую другую.
Заголовок с числом попыток у обновления из темы dead-letter хранит число доставок, через которые оно прошло, здесь 4.

## Переобработка на фильтре

<Tabs groupId="api">
<TabItem value="macros" label="Макросы">

```rust title="src/main.rs" reference="examples/guide/retries.rs#reports"
```

</TabItem>
<TabItem value="manual" label="Вручную">

```rust title="src/main.rs" reference="examples/manual/guide/retries.rs#reports"
```

</TabItem>
</Tabs>

Копия доставки на [`MqttTopic`](../components/subscriptions/mqtt-topic.md) возвращается в эту тему.
[`MqttFilter`](../components/subscriptions/mqtt-filter.md) подходит ко многим темам и ни одной не называет, поэтому его регистрация называет, куда уходит копия, а без этого сервис не запускается.
[`out_retry(Publish::default()).to(..)`](../components/settlement/out-retry.md) называет одну тему для всех копий.
`ToDeliveryTopic` вместо этого называет тему для каждой доставки: он берёт тему, на которую пришёл отчёт, из [контекста MQTT](../components/context/index.md) и отправляет копию обратно туда.
Поэтому отчёт из `devices/dev7/reports` возвращается в `devices/dev7/reports`, и только к `record`.
Обработчик принимает `Ctx<DeliveryTopic>`, и через это регистрация получает контекст MQTT. В ручной форме тип его контекста - `MqttContext`.

## Проверка задержек

Тесты в файле примера запускают приложение на внутрипроцессном сервере с остановленными часами, поэтому `tb.advance(BACKOFF)` проматывает 30 секунд мгновенно.
Они считают доставки каждого обновления, читают заголовок с числом попыток у каждой копии и проверяют, что дошло до `updates/dead`.
Работу с обвязкой описывает руководство [Тестирование сервиса на MQTT](testing.md).
