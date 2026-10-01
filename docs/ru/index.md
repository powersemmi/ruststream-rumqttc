# ruststream-rumqttc {#ruststream-rumqttc}

**`ruststream-rumqttc`** подписывает сервис [RustStream](https://powersemmi.github.io/ruststream/)
на темы MQTT 5 и публикует в них, через [`rumqttc`](https://docs.rs/rumqttc). Заголовки уходят
пользовательскими свойствами MQTT 5, поэтому клиенты не на Rust видят обычные сообщения MQTT.

Вы можете подписаться на фильтры тем с подстановочными знаками, выбрать качество обслуживания,
публиковать сохранённые сообщения, настроить сессию и последнюю волю. Качество обслуживания и флаг
сохранения объявляются один раз для издателя, а на отдельной публикации меняются там, где одному
сообщению нужно другое. Совместная подписка делит сообщения темы между конкурирующими
потребителями. С фичей `testing` тесты запускают само приложение сервиса: `MqttBroker` работает
во внутрипроцессном режиме, без сервера, или против работающего брокера.

```toml
ruststream = { version = "0.7", features = ["macros", "json"] }
ruststream-rumqttc = "0.7"
serde = { version = "1", features = ["derive"] }
```

Функция приложения называет брокер и монтирует обработчик:

```rust
--8<-- "crates/ruststream-rumqttc/examples/mqtt_service.rs:app"
```

## Куда идти дальше {#where-to-go-next}

Руководство по крейту - это его rustdoc. [Обзор крейта](https://docs.rs/ruststream-rumqttc) на
docs.rs описывает [подписку][subscribing] с двумя дескрипторами, подстановочными знаками, группами
совместной подписки и пакетами; [подтверждение][ack] и то, что значит каждый исход обработчика на
транспорте без серверных повторов; [публикацию][publishing] вместе с качеством обслуживания и
флагом сохранения отдельного сообщения; [генерируемый документ][asyncapi]; [тестирование][testing]
рабочего приложения во внутрипроцессном режиме или против живого брокера; и
[настройки соединения][operations].

<div class="grid cards" markdown>

- :material-access-point: **[Обзор крейта](https://docs.rs/ruststream-rumqttc)** - руководство по MQTT и справочник API одним документом на docs.rs.
- :material-book-open-variant: **[Документация RustStream](https://powersemmi.github.io/ruststream/)** - установка, быстрый старт, учебник и список брокеров.
- :material-language-rust: **[Справочник фреймворка](https://docs.rs/ruststream/latest/ruststream/)** - подписчики, маршрутизация, кодеки, middleware, CLI.

</div>

## Как этот сайт соотносится с документацией RustStream {#how-this-site-relates-to-the-ruststream-docs}

Этот сайт - входная страница брокера MQTT, а всё, что он раньше объяснял, теперь лежит в обзоре
крейта на docs.rs. Понятия фреймворка, которые на любом брокере работают одинаково, описаны в
[документации RustStream](https://powersemmi.github.io/ruststream/).

[subscribing]: https://docs.rs/ruststream-rumqttc/latest/ruststream_rumqttc/index.html#subscribing
[ack]: https://docs.rs/ruststream-rumqttc/latest/ruststream_rumqttc/index.html#acknowledgement
[publishing]: https://docs.rs/ruststream-rumqttc/latest/ruststream_rumqttc/index.html#publishing
[asyncapi]: https://docs.rs/ruststream-rumqttc/latest/ruststream_rumqttc/index.html#the-generated-document
[testing]: https://docs.rs/ruststream-rumqttc/latest/ruststream_rumqttc/index.html#testing
[operations]: https://docs.rs/ruststream-rumqttc/latest/ruststream_rumqttc/index.html#operations
