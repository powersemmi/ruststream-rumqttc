---
sidebar_position: 0
description: "The parts of ruststream-rumqttc an MQTT service uses, one page per component."
---

# Components

The parts of `ruststream-rumqttc` an MQTT service uses, one page per component: the broker
([`MqttBroker`](api:ruststream_rumqttc::MqttBroker)), its subscriptions, its publish policy and
the in-process transport a [`TestApp`](api:ruststream::testing::TestApp) test runs on.

- [Broker](./broker/index.md): [`MqttBroker`](api:ruststream_rumqttc::MqttBroker), its
  connection, session, last will, limits, startup and shutdown.
- [Subscriptions](./subscriptions/index.md): a topic or a filter with wildcards, its quality of
  service and share group, overlapping filters and batches.
- [Settlement](./settlement/index.md): what each handler outcome sends on MQTT, retries, the
  attempt cap and dead-letter topics.
- [Context](./context/index.md): the topic a delivery was published to.
- [Publishing](./publishing/index.md): the publish policy, the quality of service and retain flag
  of one message, headers and responders.
- [AsyncAPI](./asyncapi/index.md): the `mqtt` bindings of the generated document.
- [Testing](./testing/index.md): the production app on an in-process MQTT server, and the same
  test against a running one.
