---
sidebar_position: 0
description: "The parts of ruststream-rumqttc an MQTT service uses, one page per component."
---

# Components

The parts of `ruststream-rumqttc` an MQTT service uses, one page per component: the broker
([`MqttBroker`](api:ruststream_rumqttc::MqttBroker)), its subscriptions, its publish policy and
the in-process transport a [`TestApp`](api:ruststream::testing::TestApp) test runs on.

The [broker](./broker/index.md) pages cover its connection, session, last will, limits, startup and shutdown.
The [subscriptions](./subscriptions/index.md) read a topic or a filter with wildcards, with its quality of service and share group, overlapping filters and batches.
[Settlement](./settlement/index.md) is what each handler outcome sends on MQTT, with retries, the attempt cap and dead-letter topics.
The [context](./context/index.md) carries the topic a delivery was published to.
[Publishing](./publishing/index.md) covers the publish policy, the quality of service and retain flag of one message, headers and responders.
The [AsyncAPI](./asyncapi/index.md) pages show the `mqtt` bindings of the generated document.
[Testing](./testing/index.md) runs the production app on an in-process MQTT server, and the same test against a running one.
