//! The broker ladder: [`MqttBroker`] -> [`ConnectedMqttBroker`].
//!
//! Construction is synchronous and I/O-free; `connect` spawns the connection task and waits
//! for the broker's first `CONNACK`, and the connected form holds the live client directly.
//! One shared cell remains so publishers can be handed out while the application is still
//! being assembled, before `connect` runs.

use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;

use rumqttc::Transport;
use rumqttc::v5::mqttbytes::v5::LastWill;
use rumqttc::v5::{AsyncClient, MqttOptions};
use ruststream::{
    Broker, ConnectedBroker, DefaultPublish, DescribeServer, RedeliveryAddress, ServerSpec,
    Subscribe,
};
use tokio::sync::{OnceCell, mpsc, oneshot};

use crate::conn::{Conn, Shared, run};
use crate::error::MqttError;
use crate::filter::{MqttTopic, Qos, redelivery_topic};
use crate::publisher::{MqttPublish, MqttPublisher};
use crate::subscriber::MqttSubscriber;

/// The live connection state shared by the connected form and every handle derived from it.
pub(crate) struct Core {
    pub(crate) client: AsyncClient,
    pub(crate) shared: Arc<Shared>,
}

impl std::fmt::Debug for Core {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Core").finish_non_exhaustive()
    }
}

pub(crate) type CoreCell = Arc<OnceCell<Core>>;

/// An MQTT 5 broker for the `RustStream` messaging framework.
///
/// `new` is synchronous and records only configuration; the runtime connects once at startup
/// via the consuming [`Broker::connect`], which waits for the broker's `CONNACK`. That is
/// what lets a service compose with the synchronous `#[ruststream::app]` builder.
///
/// # Examples
///
/// ```
/// use std::time::Duration;
/// use ruststream_rumqttc::MqttBroker;
///
/// let broker = MqttBroker::new("mqtt://localhost:1883", "orders-svc")
///     .credentials("user", "pass")
///     .keep_alive(Duration::from_secs(30))
///     .clean_start(false);
/// # let _ = broker;
/// ```
#[derive(Debug, Clone)]
#[must_use]
pub struct MqttBroker {
    url: String,
    client_id: String,
    credentials: Option<(String, String)>,
    keep_alive: Option<Duration>,
    clean_start: Option<bool>,
    session_expiry: Option<u32>,
    max_packet_size: u32,
    receive_maximum: u16,
    last_will: Option<(String, Vec<u8>, Qos, bool)>,
    tls_ca: Option<Vec<u8>>,
    tls_client_auth: Option<(Vec<u8>, Vec<u8>)>,
    cell: CoreCell,
}

impl MqttBroker {
    /// Records the broker URL (`mqtt://host:port` or `mqtts://host:port`) and the client id.
    /// No I/O.
    pub fn new(url: impl Into<String>, client_id: impl Into<String>) -> Self {
        Self {
            url: url.into(),
            client_id: client_id.into(),
            credentials: None,
            keep_alive: None,
            clean_start: None,
            session_expiry: None,
            // The client's incoming default is 10 KiB, far below real payloads.
            max_packet_size: 1024 * 1024,
            receive_maximum: 1000,
            last_will: None,
            tls_ca: None,
            tls_client_auth: None,
            cell: Arc::new(OnceCell::new()),
        }
    }

    /// Username and password credentials.
    pub fn credentials(mut self, username: impl Into<String>, password: impl Into<String>) -> Self {
        self.credentials = Some((username.into(), password.into()));
        self
    }

    /// Sets the keep-alive interval (the protocol floor is 5 seconds).
    pub fn keep_alive(mut self, keep_alive: Duration) -> Self {
        self.keep_alive = Some(keep_alive);
        self
    }

    /// Starts a fresh session (`true`) or resumes a persistent one (`false`). Resuming pairs
    /// with [`session_expiry`](Self::session_expiry), and is what redelivers unacknowledged
    /// messages.
    pub fn clean_start(mut self, clean_start: bool) -> Self {
        self.clean_start = Some(clean_start);
        self
    }

    /// How long the broker keeps the session (and its subscriptions and unacked messages)
    /// after a disconnect.
    pub fn session_expiry(mut self, expiry: Duration) -> Self {
        self.session_expiry = Some(u32::try_from(expiry.as_secs()).unwrap_or(u32::MAX));
        self
    }

    /// The maximum incoming packet size. Defaults to 1 MiB (the client's own default is a
    /// 10 KiB cap that kills the connection on larger payloads).
    pub fn max_packet_size(mut self, bytes: u32) -> Self {
        self.max_packet_size = bytes;
        self
    }

    /// Flow control: how many unacknowledged `QoS` 1/2 deliveries the broker may have in
    /// flight, which is also what bounds an unread subscriber queue. Defaults to 1000.
    pub fn receive_maximum(mut self, maximum: u16) -> Self {
        self.receive_maximum = maximum;
        self
    }

    /// The last-will message the broker publishes if this session dies unexpectedly.
    pub fn last_will(
        mut self,
        topic: impl Into<String>,
        payload: impl Into<Vec<u8>>,
        qos: Qos,
        retain: bool,
    ) -> Self {
        self.last_will = Some((topic.into(), payload.into(), qos, retain));
        self
    }

    /// Trusts `ca` (PEM) for the TLS connection; selects TLS regardless of the URL scheme.
    pub fn tls_ca(mut self, ca: impl Into<Vec<u8>>) -> Self {
        self.tls_ca = Some(ca.into());
        self
    }

    /// Authenticates with a TLS client certificate (both PEM), as managed MQTT services
    /// commonly require. Implies [`tls_ca`](Self::tls_ca) must be set too.
    pub fn tls_client_auth(mut self, cert: impl Into<Vec<u8>>, key: impl Into<Vec<u8>>) -> Self {
        self.tls_client_auth = Some((cert.into(), key.into()));
        self
    }

    /// A publisher sharing this broker's connection cell; buildable before `connect`.
    #[must_use]
    pub fn publisher(&self) -> MqttPublisher {
        MqttPublisher::new(Arc::clone(&self.cell), Qos::default(), false)
    }

    /// Whether the URL's scheme selects TLS.
    fn tls_from_scheme(&self) -> bool {
        self.url.starts_with("mqtts://")
    }

    /// The URL's authority: the host and port, without the scheme, the userinfo or anything after
    /// the host.
    ///
    /// An MQTT URL routinely carries `user:password@`, and that password must reach neither the
    /// connection nor the generated document. The framework owns that trimming, so the rule is
    /// the same one every broker crate applies.
    fn authority(&self) -> String {
        ServerSpec::host_from_url(&self.url)
    }

    /// The host and port a client connects to, and what the generated document reports.
    fn endpoint(&self) -> Result<(String, u16), MqttError> {
        let authority = self.authority();
        let (host, port) = match authority.rsplit_once(':') {
            Some((host, port)) => (
                host.to_owned(),
                port.parse::<u16>()
                    .map_err(|_| MqttError::Invalid(format!("'{port}' is not a valid port")))?,
            ),
            None => (
                authority.clone(),
                if self.tls_from_scheme() { 8883 } else { 1883 },
            ),
        };
        if host.is_empty() {
            return Err(MqttError::Invalid("host must be non-empty".into()));
        }
        Ok((host, port))
    }

    fn options(&self) -> Result<MqttOptions, MqttError> {
        let tls_from_scheme = self.tls_from_scheme();
        let (host, port) = self.endpoint()?;
        if let Some(keep_alive) = self.keep_alive
            && keep_alive < Duration::from_secs(5)
        {
            return Err(MqttError::Invalid(
                "keep_alive must be at least 5 seconds".into(),
            ));
        }

        let mut options = MqttOptions::new(self.client_id.clone(), host, port);
        if let Some(keep_alive) = self.keep_alive {
            options.set_keep_alive(keep_alive);
        }
        if let Some(clean_start) = self.clean_start {
            options.set_clean_start(clean_start);
        }
        if let Some((username, password)) = &self.credentials {
            options.set_credentials(username.clone(), password.clone());
        }
        if let Some(expiry) = self.session_expiry {
            options.set_session_expiry_interval(Some(expiry));
        }
        options.set_max_packet_size(Some(self.max_packet_size));
        options.set_receive_maximum(Some(self.receive_maximum));
        options.set_manual_acks(true);
        if let Some((topic, payload, qos, retain)) = &self.last_will {
            options.set_last_will(LastWill::new(
                topic.clone(),
                payload.clone(),
                qos.to_client(),
                *retain,
                None,
            ));
        }
        if tls_from_scheme || self.tls_ca.is_some() {
            let ca = self.tls_ca.clone().unwrap_or_default();
            options.set_transport(Transport::tls(ca, self.tls_client_auth.clone(), None));
        }
        Ok(options)
    }
}

impl Broker for MqttBroker {
    type Error = MqttError;
    type Connected = ConnectedMqttBroker;

    async fn connect(self) -> Result<Self::Connected, Self::Error> {
        let core = self
            .cell
            .get_or_try_init(async || {
                let options = self.options()?;
                let (client, eventloop) = AsyncClient::new(options, 64);
                let shared = Arc::new(Shared::new());
                let (connack_tx, connack_rx) = oneshot::channel();
                tokio::spawn(run(Conn {
                    client: client.clone(),
                    eventloop,
                    shared: Arc::clone(&shared),
                    first_connack: Some(connack_tx),
                }));
                // The task retries transient failures itself; the first CONNACK (or a fatal
                // refusal) decides whether connect succeeds.
                match tokio::time::timeout(Duration::from_secs(30), connack_rx).await {
                    Ok(Ok(Ok(()))) => {}
                    Ok(Ok(Err(err))) => return Err(err),
                    Ok(Err(_)) => {
                        return Err(MqttError::Connect(Box::from(
                            "the connection task exited before the first CONNACK",
                        )));
                    }
                    Err(_) => {
                        shared.closed.store(true, Ordering::Release);
                        return Err(MqttError::Connect(Box::from(
                            "timed out waiting for the broker's CONNACK",
                        )));
                    }
                }
                Ok::<_, MqttError>(Core { client, shared })
            })
            .await?;
        Ok(ConnectedMqttBroker {
            client: core.client.clone(),
            shared: Arc::clone(&core.shared),
            cell: self.cell,
        })
    }
}

impl DescribeServer for MqttBroker {
    /// Reports the host and port a client connects to, and nothing else. A URL's credentials
    /// stay out of the generated document, which teams publish and share.
    ///
    /// The port is stated even where the URL leaves it out, because the protocol's default is
    /// what a reader of the document would otherwise have to know.
    fn describe_server(&self) -> ServerSpec {
        // A URL `connect` will reject still must not hold up the document, so the fallback keeps
        // the framework's stripped authority and drops the unusable port.
        self.endpoint().map_or_else(
            |_| ServerSpec::from_url(&self.url, "mqtt"),
            |(host, port)| ServerSpec::new(format!("{host}:{port}"), "mqtt"),
        )
    }
}

/// The typed witness that `connect` succeeded: holds the live client directly.
pub struct ConnectedMqttBroker {
    client: AsyncClient,
    shared: Arc<Shared>,
    // Keeps the cell of publishers handed out before connect alive and filled.
    cell: CoreCell,
}

impl std::fmt::Debug for ConnectedMqttBroker {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ConnectedMqttBroker")
            .finish_non_exhaustive()
    }
}

impl ConnectedMqttBroker {
    /// A publisher from the connected form with the default policy (`QoS` 1, not retained).
    #[must_use]
    pub fn publisher(&self) -> MqttPublisher {
        MqttPublisher::new(Arc::clone(&self.cell), Qos::default(), false)
    }

    pub(crate) fn publisher_with(&self, policy: MqttPublish) -> MqttPublisher {
        policy.into_publisher(Arc::clone(&self.cell))
    }

    /// Opens the subscription described by `topic` and waits for the broker's `SUBACK`.
    ///
    /// # Errors
    ///
    /// Returns [`MqttError`] when the descriptor is invalid, the broker rejects the filter,
    /// or the broker is shut down.
    pub async fn subscribe_topic(&self, topic: MqttTopic) -> Result<MqttSubscriber, MqttError> {
        topic.validate()?;
        self.shared.ensure_open()?;

        let wire_filter = topic.wire_filter();
        let (tx, rx) = mpsc::unbounded_channel();
        let (done, wait) = oneshot::channel();
        let id = self.shared.register(
            wire_filter.clone(),
            topic.filter().to_owned(),
            topic.qos_value().to_client(),
            tx,
            done,
        );
        if self
            .client
            .subscribe(wire_filter, topic.qos_value().to_client())
            .await
            .is_err()
        {
            self.shared.remove(id);
            return Err(MqttError::Subscribe {
                filter: topic.filter().to_owned(),
                reason: "the mqtt connection task has shut down".to_owned(),
            });
        }
        wait.await.map_err(|_| MqttError::Subscribe {
            filter: topic.filter().to_owned(),
            reason: "the mqtt connection task has shut down".to_owned(),
        })??;

        Ok(MqttSubscriber::new(
            topic.filter().to_owned(),
            id,
            Arc::clone(&self.shared),
            self.client.clone(),
            rx,
        ))
    }
}

impl ConnectedBroker for ConnectedMqttBroker {
    type Error = MqttError;
    type Closed = ();

    async fn shutdown(self) -> Result<(), Self::Error> {
        self.shared.closed.store(true, Ordering::Release);
        let held = self.shared.held();
        if held > 0 {
            // Unacknowledged, so the broker redelivers them when the session resumes; they are
            // lost only if the session is not persistent.
            tracing::warn!(
                held,
                "mqtt shutdown with deliveries no subscription ever matched"
            );
        }
        // A clean DISCONNECT lets the broker publish no last will and expire the session per
        // policy; the connection task sees the closed flag and exits.
        let _ = self.client.disconnect().await;
        Ok(())
    }
}

impl Subscribe for ConnectedMqttBroker {
    type Subscriber = MqttSubscriber;

    async fn subscribe(&self, name: &str) -> Result<Self::Subscriber, Self::Error> {
        self.subscribe_topic(MqttTopic::new(name)).await
    }

    /// A concrete topic filter is also the topic a publisher names, so a deferred retry reaches
    /// the subscription that reported it. A wildcard filter is subscribe-only and reports none.
    fn redelivery_address(&self, name: &str) -> Option<RedeliveryAddress> {
        redelivery_topic(name)
    }
}

impl DefaultPublish for ConnectedMqttBroker {
    type Policy = MqttPublish;
}

#[cfg(test)]
mod tests {
    use super::*;

    fn broker(url: &str) -> MqttBroker {
        MqttBroker::new(url, "describe")
    }

    #[test]
    fn the_endpoint_is_the_host_and_port_whatever_else_the_url_carries() {
        for (url, expected) in [
            ("mqtt://broker.example.com", ("broker.example.com", 1883)),
            ("mqtts://broker.example.com", ("broker.example.com", 8883)),
            (
                "mqtt://broker.example.com:1884",
                ("broker.example.com", 1884),
            ),
            (
                "mqtt://alice:s3cret@broker.example.com:1884",
                ("broker.example.com", 1884),
            ),
            (
                "mqtts://alice:s3cret@broker.example.com",
                ("broker.example.com", 8883),
            ),
            // A password may contain the separator, so the split takes the last one.
            (
                "mqtt://alice:p@ss@broker.example.com:1884",
                ("broker.example.com", 1884),
            ),
            (
                "mqtt://broker.example.com:1884/ns",
                ("broker.example.com", 1884),
            ),
            ("mqtt://broker.example.com/ns", ("broker.example.com", 1883)),
            // The authority ends before the path, so an `@` after it is not a separator.
            (
                "mqtt://broker.example.com/a@b",
                ("broker.example.com", 1883),
            ),
        ] {
            let broker = broker(url);
            let (host, port) = broker.endpoint().expect("the url parses");
            assert_eq!((host.as_str(), port), expected, "parsing {url}");
        }
    }

    #[test]
    fn a_url_with_credentials_describes_a_server_without_them() {
        let spec = broker("mqtt://alice:s3cret@broker.example.com:1884").describe_server();
        let host = spec.host.expect("a networked broker states its host");

        assert_eq!(host, "broker.example.com:1884");
        assert!(
            !host.contains('@'),
            "the userinfo separator is gone: {host}"
        );
        assert!(!host.contains("alice"), "the user name is gone: {host}");
        assert!(!host.contains("s3cret"), "the password is gone: {host}");
    }
}
