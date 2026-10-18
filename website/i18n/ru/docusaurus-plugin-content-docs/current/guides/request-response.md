---
title: Запрос и ответ
sidebar_position: 7
description: "Ответ на запросы MQTT 5 в тему ответа, которую называет каждый запрос, с его данными корреляции, и запасная тема для запроса, который её не называет."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Запрос и ответ

Это руководство отвечает на запросы MQTT 5: каждый ответ уходит в тему, которую называет его запрос, и содержит данные корреляции запроса.

Для этого MQTT 5 даёт запросу два свойства.
Тема ответа указывает, куда уходит ответ.
Данные корреляции - непрозрачное значение, по которому запрашивающая сторона сопоставляет ответ со своим вопросом.
Брокер отображает их на заголовки `reply-to` и `correlation-id` в обе стороны.

Пример - агент устройства.
Консоли запрашивают у него одну настройку в `devices/dev42/config/get`, а он отвечает её значением.

## Отвечающая сторона

<Tabs groupId="api">
<TabItem value="macros" label="Макросы">

```rust title="src/main.rs" reference="examples/guide/request_response.rs#responder"
```

</TabItem>
<TabItem value="manual" label="Вручную">

```rust title="src/main.rs" reference="examples/manual/guide/request_response.rs#responder"
```

</TabItem>
</Tabs>

[Отвечающая сторона](../components/publishing/responders.md) - это обычный обработчик с ответом: значение, которое он возвращает, публикуется.
`ConfigValue` не называет темы, поэтому её называет точка монтирования: `reply("devices/dev42/config")`.
Эта тема - запасная, для запроса без темы ответа.
Запрос настройки, которой у устройства нет, заканчивается `drop`, и ничего не публикуется.

## Ответ в тему ответа

<Tabs groupId="api">
<TabItem value="macros" label="Макросы">

```rust title="src/main.rs" reference="examples/guide/request_response.rs#transform"
```

</TabItem>
<TabItem value="manual" label="Вручную">

```rust title="src/main.rs" reference="examples/manual/guide/request_response.rs#transform"
```

</TabItem>
</Tabs>

`ToResponseTopic` - преобразование публикации в позиции ответа.
Оно читает заголовки запроса: `reply-to` становится темой ответа, а `correlation-id` копируется в ответ.
Оно объявляет `Destination = Names`, и это даёт ему право выбирать тему.
Запрос без `reply-to` оставляет тему точки монтирования.

[Документ AsyncAPI](../components/asyncapi/replies.md) сервиса указывает адресом ответа заголовок `reply-to` запроса, `$message.header#/reply-to`, потому что ответы не собираются ни в одном фиксированном канале.

## Запрос из командной строки

Запрашивающей стороной может быть любой клиент MQTT 5.
В Mosquitto входит `mosquitto_rr`: он публикует запрос с темой ответа и печатает ответ:

```bash
docker compose exec mosquitto mosquitto_rr \
  -t devices/dev42/config/get -e consoles/c1/responses -m '{"key":"firmware"}'
```

Он печатает `{"key":"firmware","value":"2.1.0"}`.
Запрашивающая сторона внутри сервиса на RustStream публикует запрос с обоими заголовками и читает ответы подписчиком на собственную тему ответа.

## Проверка отвечающей стороны

Тесты в файле примера публикуют на внутрипроцессном сервере запрос с заголовками `reply-to` и `correlation-id`.
Заголовки доходят до обработчика через то же отображение на свойства MQTT 5, что и при передаче по сети.
Тесты проверяют ответ в `consoles/c1/responses` с данными корреляции, запасную тему для запроса без темы ответа и то, что на неизвестную настройку ответа нет.
