# ruststream-rumqttc {#ruststream-rumqttc}

**`ruststream-rumqttc`** подписывает сервис [RustStream](https://powersemmi.github.io/ruststream/)
на темы MQTT 5 и публикует в них, через [`rumqttc`](https://docs.rs/rumqttc). Заголовки уходят
пользовательскими свойствами MQTT 5, поэтому клиенты не на Rust видят обычные сообщения MQTT.

Вы можете подписаться на фильтры тем с подстановочными знаками, выбрать качество обслуживания,
публиковать сохранённые сообщения, настроить сессию и последнюю волю. Качество обслуживания и флаг
сохранения объявляются один раз для издателя, а на отдельной публикации меняются там, где одному
сообщению нужно другое. Совместная подписка делит сообщения темы между конкурирующими
потребителями. С фичей `testing` вы можете прогнать обработчики сервиса на внутрипроцессном
брокере, без сервера.

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

<div class="grid cards" markdown>

- :material-access-point: **[Руководство по MQTT](mqtt.md)** - фильтры тем, качество обслуживания, совместные подписки, публикация с сохранением и тестирование.
- :material-book-open-variant: **[Документация RustStream](https://powersemmi.github.io/ruststream/)** - сам фреймворк: подписчики, маршрутизация, кодеки, middleware, CLI.
- :material-language-rust: **[Справочник API](https://docs.rs/ruststream-rumqttc)** - rustdoc крейта на docs.rs.

</div>

## Как этот сайт соотносится с документацией RustStream {#how-this-site-relates-to-the-ruststream-docs}

Этот сайт описывает брокер MQTT. Понятия фреймворка, которые на любом брокере работают одинаково,
описаны в [документации RustStream](https://powersemmi.github.io/ruststream/).
