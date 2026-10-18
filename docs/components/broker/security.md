---
sidebar_position: 1
title: Security
description: "The URL's login or credentials sends a user name and a password; tls_ca trusts a certificate authority, and tls_client_auth adds a client certificate."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Security

The login in the URL or [`credentials`](api:ruststream_rumqttc::MqttBroker::credentials) sends a user name and a password; [`tls_ca`](api:ruststream_rumqttc::MqttBroker::tls_ca) trusts a certificate authority and selects TLS, and [`tls_client_auth`](api:ruststream_rumqttc::MqttBroker::tls_client_auth) adds a client certificate.

The user name and the password travel in the `CONNECT` packet.
A server that refuses them fails [startup](./startup.md) at once, naming the refusal.
A URL such as `mqtt://user:password@host:1884` logs in with the user name and the password it carries.
They are percent-decoded, as a URL writes them, so `p%40ss` is the password `p@ss`.
The URL logs in only when it carries both a non-empty user name and a password.
`credentials` is the explicit choice and wins over a login in the URL.

TLS takes PEM bytes, so the service reads its files itself and hands their contents over.
`tls_ca` is the certificate authority that signed the server's certificate.
The client verifies the server against it and against no other store.
So a TLS connection needs it: an `mqtts://` URL without `tls_ca` fails startup, naming the missing setting.
`tls_ca` also selects TLS on an `mqtt://` URL.

`tls_client_auth` takes the service's own certificate and key, for a server that verifies each client by its certificate, as managed MQTT services commonly do.
A server may ask for both: a client certificate on a TLS listener, and a user name and a password inside the session.

The login and the handshake happen on the server, so a [`TestApp`](../testing/index.md) test runs the same app in process without them.
The settings are still checked as at the connect: an `mqtts://` URL without `tls_ca` fails `TestApp::start` too.
The generated [AsyncAPI document](../asyncapi/server.md) reports the host and the port, never the credentials.

## Example

The service logs in with its user name and the password the environment gives it.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/mqtt_broker_security.rs#password"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/mqtt_broker_security.rs#password"
```

</TabItem>
</Tabs>

A deployment that hands over one connection string passes it as it is.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/mqtt_broker_security.rs#url"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/mqtt_broker_security.rs#url"
```

</TabItem>
</Tabs>

On a TLS listener that verifies clients by their certificates, the service hands over the three PEM files of its deployment.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/mqtt_broker_security.rs#tls"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/mqtt_broker_security.rs#tls"
```

</TabItem>
</Tabs>
