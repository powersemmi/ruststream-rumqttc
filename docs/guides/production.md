---
sidebar_position: 9
description: "Connect an MQTT service to a managed broker: credentials, TLS with a client certificate, one client id per process, limits, reconnects and a configuration test."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Connecting to production

A managed MQTT service, such as AWS IoT Core, HiveMQ Cloud or EMQX Cloud, asks for [credentials and TLS](../components/broker/security.md), often with a client certificate.
This guide configures the broker for one, from the deployment's environment.

## Credentials and TLS

{/* single-form: the broker's configuration is the same code with and without the macros */}
```rust title="src/main.rs" reference="examples/guide/production.rs#broker"
```

The deployment provides every setting in environment variables, and the certificates as files.
[`credentials`](api:ruststream_rumqttc::MqttBroker::credentials) sets the user name and the password of the `CONNECT` packet.
They go through this call only, never inside the URL.
They stay out of the generated AsyncAPI document, which reports only the host and port the service connects to.

`mqtts://` selects TLS, on port 8883 when the URL names none.
[`tls_ca`](api:ruststream_rumqttc::MqttBroker::tls_ca) is the certificate authority, in PEM, that the client verifies the server against.
The client trusts that authority and no other store, so a `mqtts://` URL without `tls_ca` fails the start, naming `tls_ca`.
[`tls_client_auth`](api:ruststream_rumqttc::MqttBroker::tls_client_auth) adds the client's certificate and key, for a service that authenticates clients by certificate.

## One client id per process

The client id is the second argument of `MqttBroker::new`, and the server keys the session by it.
A second connection with the same id takes the session over and closes the first one.
So two replicas of a service never share an id: here each takes its own from `MQTT_CLIENT_ID`, for example the pod's name.
Replicas that split the work join a [share group](topics.md) instead.

## Limits

- [`keep_alive`](../components/broker/limits.md) is how often the client proves it is alive on an idle connection; the server closes a connection silent for one and a half times as long. The `rumqttc` client's minimum is 5 seconds, and its default is 60.
- [`max_packet_size`](api:ruststream_rumqttc::MqttBroker::max_packet_size) is the largest packet the service accepts; the server does not send it a larger one. The broker's default is 1 MiB, above the client's own default of 10 KiB.
- [`receive_maximum`](api:ruststream_rumqttc::MqttBroker::receive_maximum) bounds the `QoS` 1 and 2 deliveries in flight ([Delivery guarantees](delivery.md)).

## The service

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust title="src/main.rs" reference="examples/guide/production.rs#service"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust title="src/main.rs" reference="examples/manual/guide/production.rs#service"
```

</TabItem>
</Tabs>

The broker connects once the service starts.
A lost connection [reconnects](../components/broker/reconnect.md) with a backoff that starts at 100 ms and doubles up to 5 seconds, and the broker subscribes again when the server reports the session gone.
A refusal the connection will not outgrow ends the connection task instead: bad credentials, a client id the server does not accept, or a TLS handshake the certificates cannot satisfy.
Every subscription then returns that error.

## Testing the configuration

The tests start the production `service` with the production `broker`, given test settings, under `TestApp::start`.
The in-process server reads the broker's configuration and refuses what the client would refuse, without dialling the server:

- a keep-alive under 5 seconds fails the start with the client's minimum in the message;
- a `mqtts://` URL without `tls_ca` fails the start, naming `tls_ca`.

The credentials and the TLS handshake are checked by a real server only.
The live tests of the [Security](../components/broker/security.md) page run both against Mosquitto listeners that demand them.
