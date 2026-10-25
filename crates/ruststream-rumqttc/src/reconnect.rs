//! What a service does when its connection is lost after startup: [`Reconnect`], chosen on the
//! broker with [`MqttBroker::reconnect`].
//!
//! The mode acts only after the connection task has seen a failure, so a delivery pays nothing
//! for any of them; a [custom](Reconnect::custom) reconnection is called through dynamic dispatch
//! on that failure path alone.

use std::fmt;
use std::future::Future;
use std::num::NonZeroU32;
use std::sync::Arc;
use std::time::Duration;

use futures::future::BoxFuture;
use rumqttc::v5::mqttbytes::v5::Packet;
use rumqttc::v5::{ConnectionError, Event, StateError};
use tokio::sync::oneshot;

use crate::broker::MqttBroker;
use crate::conn::{Awaiting, Conn, Flow, PendingSub, fatal_reason};
use crate::error::MqttError;
use crate::filter::Qos;

/// How a service handles a connection lost after startup.
///
/// The default is [`automatic`](Self::automatic) with [`AutoReconnect::new`]: the client dials
/// again on its own, pausing between attempts, and the crate subscribes again in a session the
/// server did not keep. [`never`](Self::never) ends every subscription's stream instead, and
/// [`custom`](Self::custom) hands the recovery to a [`Reconnector`] of the service's own.
///
/// A refusal that retrying cannot change ends every stream in every mode: the server refusing the
/// login or the authorization, refusing the `CONNECT` as malformed or unsupported, or a TLS
/// handshake that fails. Every other end of the connection is a lost connection the mode handles,
/// the `DISCONNECT` a server sends when it shuts down or when the keep-alive runs out among them.
/// A mode that gives up ends every subscription's stream after an error naming the reason, and
/// the service's [`SubscriptionEnd`](ruststream::runtime::SubscriptionEnd) setting decides what
/// happens next.
///
/// The mode covers the connection after its first `CONNACK`. Until then the connection task
/// retries what is not fatal, and [`connect`](ruststream::Broker::connect) gives up after 30
/// seconds.
///
/// # Examples
///
/// ```
/// # mod demo {
/// use std::num::NonZeroU32;
/// use std::time::Duration;
///
/// use ruststream::runtime::SubscriptionEnd;
/// use ruststream_rumqttc::prelude::*;
/// use serde::Deserialize;
///
/// const ATTEMPTS: NonZeroU32 = NonZeroU32::new(20).unwrap();
///
/// #[derive(Deserialize)]
/// struct Telemetry {
///     device: String,
/// }
///
/// #[subscriber(MqttTopic::new("devices/telemetry"))]
/// async fn record(telemetry: &Telemetry) -> HandlerOutcome {
///     tracing::info!(telemetry.device, "reading recorded");
///     HandlerOutcome::ack()
/// }
///
/// #[ruststream::app]
/// fn app() -> impl App {
///     // Twenty dials, a second apart at first and ten at most, then the stream ends and the
///     // service stops with an error.
///     let reconnect = Reconnect::automatic(
///         AutoReconnect::new()
///             .pauses(Duration::from_secs(1), Duration::from_secs(10))
///             .attempts(ATTEMPTS),
///     );
///     RustStream::new(AppInfo::new("telemetry", "0.1.0"))
///         .on_subscription_end(SubscriptionEnd::FailFast)
///         .with_broker(
///             MqttBroker::new("mqtt://localhost:1883", "telemetry-svc").reconnect(reconnect),
///             |b| {
///                 b.include(record);
///             },
///         )
/// }
/// # }
/// # fn main() {}
/// ```
#[derive(Clone)]
#[must_use]
pub struct Reconnect {
    mode: Mode,
}

#[derive(Clone)]
enum Mode {
    Automatic(AutoReconnect),
    Never,
    Custom(Arc<dyn ErasedReconnector>),
}

impl Reconnect {
    /// The client's own reconnection: the connection task dials again after a pause, as `settings`
    /// say, and subscribes again every filter a session the server did not keep took with it.
    pub const fn automatic(settings: AutoReconnect) -> Self {
        Self {
            mode: Mode::Automatic(settings),
        }
    }

    /// No reconnection: a lost connection ends every subscription's stream, and publishes fail
    /// from then on.
    pub const fn never() -> Self {
        Self { mode: Mode::Never }
    }

    /// The service's own reconnection: `reconnector` owns the whole recovery of every lost
    /// connection (see [`Reconnector`]).
    pub fn custom(reconnector: impl Reconnector) -> Self {
        Self {
            mode: Mode::Custom(Arc::new(reconnector)),
        }
    }
}

impl Default for Reconnect {
    fn default() -> Self {
        Self::automatic(AutoReconnect::new())
    }
}

impl fmt::Debug for Reconnect {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.mode {
            Mode::Automatic(settings) => f.debug_tuple("Automatic").field(settings).finish(),
            Mode::Never => f.write_str("Never"),
            Mode::Custom(_) => f.write_str("Custom"),
        }
    }
}

/// The settings of [`Reconnect::automatic`]: the pauses between dials and how many dials a lost
/// connection gets.
///
/// The first pause follows the loss, each later one doubles up to the longest. The defaults are
/// 100 ms, 5 s and no limit.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[must_use]
pub struct AutoReconnect {
    first_pause: Duration,
    longest_pause: Duration,
    attempts: Option<NonZeroU32>,
}

impl AutoReconnect {
    /// Pauses from 100 ms up to 5 s, and dials until the connection is back.
    pub const fn new() -> Self {
        Self {
            first_pause: Duration::from_millis(100),
            longest_pause: Duration::from_secs(5),
            attempts: None,
        }
    }

    /// Pauses `first` before the first dial after a loss, and doubles the pause after each failed
    /// dial up to `longest`. A `first` longer than `longest` pauses `longest` every time.
    pub const fn pauses(mut self, first: Duration, longest: Duration) -> Self {
        self.first_pause = first;
        self.longest_pause = longest;
        self
    }

    /// Gives up after `attempts` failed dials in a row, which ends every subscription's stream.
    pub const fn attempts(mut self, attempts: NonZeroU32) -> Self {
        self.attempts = Some(attempts);
        self
    }

    fn first(&self) -> Duration {
        self.first_pause.min(self.longest_pause)
    }

    fn next(&self, pause: Duration) -> Duration {
        pause.saturating_mul(2).min(self.longest_pause)
    }
}

impl Default for AutoReconnect {
    fn default() -> Self {
        Self::new()
    }
}

/// A reconnection of the service's own, installed with [`Reconnect::custom`].
///
/// The connection task calls [`reconnect`](Self::reconnect) once per lost connection and reads
/// no packet until it returns. The implementation owns the whole recovery through the
/// [`Recovery`] it is handed: the broker's configuration and why the connection ended, a dial
/// that installs the new connection publishers use from then on, every open subscription, and
/// the helpers that subscribe one again as it was declared. It decides how to reconnect, what to
/// restore and in which order, or that it gives up.
///
/// Returning the [`Reconnected`] that [`Recovery::connect`] produced resumes the service on the
/// new connection. Returning an error gives up: every subscription's stream ends with it. A
/// refusal that retrying cannot change gives up whatever the implementation returns.
///
/// # Examples
///
/// ```
/// # mod demo {
/// use std::time::Duration;
///
/// use ruststream_rumqttc::prelude::*;
/// use ruststream_rumqttc::{MqttError, Reconnected, Reconnector, Recovery};
/// use serde::Deserialize;
///
/// /// Dials with a fresh token every five seconds for a minute, then restores every
/// /// subscription.
/// struct FreshToken;
///
/// impl FreshToken {
///     fn token(&self) -> String {
///         "token-from-the-vault".to_owned()
///     }
/// }
///
/// impl Reconnector for FreshToken {
///     async fn reconnect(&self, recovery: &mut Recovery<'_>) -> Result<Reconnected, MqttError> {
///         tracing::warn!(reason = %recovery.lost(), "mqtt connection lost");
///         let mut attempt = 0;
///         let reconnected = loop {
///             recovery.credentials("telemetry-svc", self.token());
///             match recovery.connect().await {
///                 Ok(reconnected) => break reconnected,
///                 Err(err) if attempt == 12 => return Err(err),
///                 Err(_) => attempt += 1,
///             }
///             tokio::time::sleep(Duration::from_secs(5)).await;
///         };
///         if !reconnected.session_present() {
///             recovery.restore_all().await?;
///         }
///         Ok(reconnected)
///     }
/// }
///
/// #[derive(Deserialize)]
/// struct Telemetry {
///     device: String,
/// }
///
/// #[subscriber(MqttTopic::new("devices/telemetry"))]
/// async fn record(telemetry: &Telemetry) -> HandlerOutcome {
///     tracing::info!(telemetry.device, "reading recorded");
///     HandlerOutcome::ack()
/// }
///
/// #[ruststream::app]
/// fn app() -> impl App {
///     RustStream::new(AppInfo::new("telemetry", "0.1.0")).with_broker(
///         MqttBroker::new("mqtt://localhost:1883", "telemetry-svc")
///             .reconnect(Reconnect::custom(FreshToken)),
///         |b| {
///             b.include(record);
///         },
///     )
/// }
/// # }
/// # fn main() {}
/// ```
pub trait Reconnector: Send + Sync + 'static {
    /// Recovers one lost connection, or gives up with an error.
    ///
    /// # Errors
    ///
    /// The error ends every subscription's stream.
    fn reconnect(
        &self,
        recovery: &mut Recovery<'_>,
    ) -> impl Future<Output = Result<Reconnected, MqttError>> + Send;
}

/// [`Reconnector`] behind a pointer the broker can clone: the boxed future is paid once per lost
/// connection.
pub(crate) trait ErasedReconnector: Send + Sync {
    fn reconnect<'a>(
        &'a self,
        recovery: &'a mut Recovery<'_>,
    ) -> BoxFuture<'a, Result<Reconnected, MqttError>>;
}

impl<R: Reconnector> ErasedReconnector for R {
    fn reconnect<'a>(
        &'a self,
        recovery: &'a mut Recovery<'_>,
    ) -> BoxFuture<'a, Result<Reconnected, MqttError>> {
        Box::pin(Reconnector::reconnect(self, recovery))
    }
}

/// Why a connection ended: what the client read, and the reason code of the server's
/// `DISCONNECT` when the server sent one.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LostConnection {
    server_reason: Option<u8>,
    message: String,
}

impl LostConnection {
    pub(crate) fn new(err: &ConnectionError) -> Self {
        let server_reason = match err {
            ConnectionError::MqttState(StateError::ServerDisconnect { reason_code, .. }) => {
                Some(*reason_code as u8)
            }
            _ => None,
        };
        Self {
            server_reason,
            message: err.to_string(),
        }
    }

    /// The reason code of the `DISCONNECT` the server ended the connection with: `0x8B` for a
    /// server shutting down, `0x8D` for a keep-alive timeout. `None` when the connection ended
    /// without one.
    #[must_use]
    pub const fn server_reason_code(&self) -> Option<u8> {
        self.server_reason
    }
}

impl fmt::Display for LostConnection {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.message)
    }
}

/// The proof that [`Recovery::connect`] reached a `CONNACK`, which is what
/// [`Reconnector::reconnect`] returns to resume the service.
#[derive(Debug)]
pub struct Reconnected {
    session_present: bool,
}

impl Reconnected {
    /// Whether the server kept the session, and with it every subscription and every message it
    /// queued meanwhile. A session it did not keep has no subscription until one is restored.
    #[must_use]
    pub const fn session_present(&self) -> bool {
        self.session_present
    }
}

/// One subscription open when the connection was lost, as [`Recovery::subscriptions`] lists it.
///
/// Its stream stays attached: whatever the server delivers for its filter on the new connection
/// reaches it, once the filter is on the server again.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LostSubscription {
    filter: String,
    qos: Qos,
}

impl LostSubscription {
    /// The filter as subscribed, `$share/<group>/` included for a share group.
    #[must_use]
    pub fn filter(&self) -> &str {
        &self.filter
    }

    /// The quality of service the filter is subscribed at.
    #[must_use]
    pub const fn qos(&self) -> Qos {
        self.qos
    }
}

/// Everything a [`Reconnector`] recovers a lost connection with.
///
/// The connection belongs to the recovery until `reconnect` returns: a dial, a restore and a wait
/// for an answer all run here, and nothing else reads from the server meanwhile.
pub struct Recovery<'a> {
    conn: &'a mut Conn,
    broker: &'a MqttBroker,
    lost: LostConnection,
    session: Option<bool>,
    fatal: Option<String>,
}

impl fmt::Debug for Recovery<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Recovery")
            .field("lost", &self.lost)
            .field("connected", &self.session.is_some())
            .finish_non_exhaustive()
    }
}

impl Recovery<'_> {
    /// The broker as the service configured it.
    #[must_use]
    pub const fn broker(&self) -> &MqttBroker {
        self.broker
    }

    /// Why the connection ended, or why the last dial of this recovery failed.
    #[must_use]
    pub const fn lost(&self) -> &LostConnection {
        &self.lost
    }

    /// The user name and password every later dial logs in with.
    pub fn credentials(&mut self, username: impl Into<String>, password: impl Into<String>) {
        self.conn
            .eventloop
            .options
            .set_credentials(username.into(), password.into());
    }

    /// Dials the server once and waits for its `CONNACK`. The new connection is the one every
    /// publisher and every acknowledgement uses from then on. Called on a recovery that is
    /// connected already, it answers the connection it has.
    ///
    /// # Errors
    ///
    /// Returns [`MqttError::Connect`] when the dial fails, and [`MqttError::NotConnected`] once the
    /// broker is shutting down. A refusal that retrying cannot change keeps failing every later
    /// call, and the recovery gives up with it.
    pub async fn connect(&mut self) -> Result<Reconnected, MqttError> {
        loop {
            if let Some(session_present) = self.session {
                return Ok(Reconnected { session_present });
            }
            self.usable()?;
            let event = self.poll().await?;
            if let Event::Incoming(Packet::ConnAck(connack)) = &event {
                self.session = Some(connack.session_present);
            }
            self.handle(event)?;
        }
    }

    /// Every subscription open when the connection was lost, in the order they were opened.
    #[must_use]
    pub fn subscriptions(&self) -> Vec<LostSubscription> {
        self.conn
            .shared
            .wires()
            .into_iter()
            .map(|(filter, qos)| LostSubscription {
                filter,
                qos: Qos::from_client(qos),
            })
            .collect()
    }

    /// Subscribes `subscription`'s filter again as it was declared, and waits for the server's
    /// answer. A subscription that closed meanwhile is left alone.
    ///
    /// # Errors
    ///
    /// Returns [`MqttError::Subscribe`] when the server refuses the filter,
    /// [`MqttError::NotConnected`] before [`connect`](Self::connect) succeeded or once the broker
    /// is shutting down, and [`MqttError::Connect`] when the connection is lost again meanwhile.
    pub async fn restore(&mut self, subscription: &LostSubscription) -> Result<(), MqttError> {
        if self.session.is_none() {
            return Err(MqttError::NotConnected);
        }
        let (done, mut answer) = oneshot::channel();
        let Some(request) = self.conn.shared.restore_request(&subscription.filter) else {
            return Ok(());
        };
        let mut record = PendingSub {
            filter: request.filter.clone(),
            pkid: None,
            awaiting: Awaiting::Restore { done },
        };
        // The client's request queue drains only as this recovery polls, so a full one is polled
        // until it takes the subscribe.
        while let Err(back) = self
            .conn
            .shared
            .send_subscribe(&self.conn.client, &request, record)
        {
            record = back;
            self.step().await?;
        }
        loop {
            if let Ok(outcome) = answer.try_recv() {
                return outcome;
            }
            if let Err(err) = self.step().await {
                self.conn.shared.abandon_restores();
                return Err(err);
            }
        }
    }

    /// [`restore`](Self::restore)s every subscription in the order they were opened.
    ///
    /// # Errors
    ///
    /// The first error a restore returned; the subscriptions after it are not restored.
    pub async fn restore_all(&mut self) -> Result<(), MqttError> {
        for subscription in self.subscriptions() {
            self.restore(&subscription).await?;
        }
        Ok(())
    }

    /// Ends the stream of every subscription opened on `subscription`'s filter, with an error
    /// naming why the recovery left it behind.
    pub fn end(&mut self, subscription: &LostSubscription) {
        self.conn.shared.end_filter(
            &subscription.filter,
            "the reconnection did not restore this subscription",
        );
    }

    /// Fails while the recovery cannot use the connection any more.
    fn usable(&self) -> Result<(), MqttError> {
        if let Some(reason) = &self.fatal {
            return Err(MqttError::Connect(Box::from(reason.clone())));
        }
        self.conn.shared.ensure_open()
    }

    /// Reads one event from the connection, and handles it.
    async fn step(&mut self) -> Result<(), MqttError> {
        self.usable()?;
        let event = self.poll().await?;
        self.handle(event)
    }

    async fn poll(&mut self) -> Result<Event, MqttError> {
        match self.conn.eventloop.poll().await {
            Ok(event) => Ok(event),
            Err(err) => {
                self.session = None;
                if let Some(reason) = fatal_reason(&err) {
                    self.fatal = Some(reason.clone());
                    return Err(MqttError::Connect(Box::from(reason)));
                }
                self.conn.lost(&err);
                self.lost = LostConnection::new(&err);
                Err(MqttError::Connect(Box::from(self.lost.message.clone())))
            }
        }
    }

    fn handle(&mut self, event: Event) -> Result<(), MqttError> {
        match self.conn.handle(event) {
            Flow::Continue => Ok(()),
            Flow::Stop => Err(MqttError::NotConnected),
        }
    }
}

/// How the connection task's recovery of a lost connection ended.
pub(crate) enum Recovered {
    /// The connection is back.
    Resumed,
    /// The mode gave up, for the reason given; every stream ends with it.
    GaveUp(String),
    /// The broker is shutting down.
    Closing,
}

/// The mode a connection task recovers lost connections with, resolved from [`Reconnect`] at
/// `connect`.
pub(crate) enum Policy {
    Automatic(AutoReconnect),
    Never,
    Custom {
        reconnector: Arc<dyn ErasedReconnector>,
        broker: Box<MqttBroker>,
    },
}

impl Policy {
    pub(crate) fn new(broker: &MqttBroker) -> Self {
        match &broker.reconnect_mode().mode {
            Mode::Automatic(settings) => Self::Automatic(*settings),
            Mode::Never => Self::Never,
            Mode::Custom(reconnector) => Self::Custom {
                reconnector: Arc::clone(reconnector),
                broker: Box::new(broker.clone()),
            },
        }
    }

    /// Whether a `CONNACK` of a session the server did not keep subscribes every filter again on
    /// its own. A custom reconnection restores what it chooses.
    pub(crate) const fn restores_on_connack(&self) -> bool {
        !matches!(self, Self::Custom { .. })
    }
}

/// Recovers the connection `conn` lost with `err`, as its policy says.
pub(crate) async fn recover(conn: &mut Conn, err: &ConnectionError) -> Recovered {
    let lost = LostConnection::new(err);
    let shared = Arc::clone(&conn.shared);
    let recovery = async {
        match &conn.policy {
            Policy::Never => Recovered::GaveUp(format!(
                "the mqtt connection was lost and the broker does not reconnect: {lost}"
            )),
            Policy::Automatic(settings) => {
                let settings = *settings;
                automatic(conn, settings, &lost).await
            }
            Policy::Custom {
                reconnector,
                broker,
            } => {
                let reconnector = Arc::clone(reconnector);
                let broker = broker.clone();
                custom(conn, &*reconnector, &broker, lost.clone()).await
            }
        }
    };
    tokio::select! {
        recovered = recovery => recovered,
        () = shared.closing() => Recovered::Closing,
    }
}

async fn automatic(conn: &mut Conn, settings: AutoReconnect, lost: &LostConnection) -> Recovered {
    let mut pause = settings.first();
    let mut failed = 0u32;
    loop {
        tokio::time::sleep(pause).await;
        match conn.eventloop.poll().await {
            Ok(event) => {
                return match conn.handle(event) {
                    Flow::Continue => Recovered::Resumed,
                    Flow::Stop => Recovered::Closing,
                };
            }
            Err(err) => {
                if conn.shared.gone() {
                    return Recovered::Closing;
                }
                if let Some(reason) = fatal_reason(&err) {
                    return Recovered::GaveUp(reason);
                }
                conn.lost(&err);
                failed += 1;
                if settings
                    .attempts
                    .is_some_and(|attempts| failed >= attempts.get())
                {
                    return Recovered::GaveUp(format!(
                        "the mqtt connection was lost ({lost}) and {failed} dials failed, the \
                         last with: {err}"
                    ));
                }
                tracing::debug!(error = %err, attempt = failed, "mqtt reconnect failed; pausing");
                pause = settings.next(pause);
            }
        }
    }
}

async fn custom(
    conn: &mut Conn,
    reconnector: &dyn ErasedReconnector,
    broker: &MqttBroker,
    lost: LostConnection,
) -> Recovered {
    let mut recovery = Recovery {
        conn,
        broker,
        lost,
        session: None,
        fatal: None,
    };
    let outcome = reconnector.reconnect(&mut recovery).await;
    if let Some(reason) = recovery.fatal {
        return Recovered::GaveUp(reason);
    }
    if recovery.conn.shared.gone() {
        return Recovered::Closing;
    }
    match outcome {
        Ok(_) => Recovered::Resumed,
        Err(err) => Recovered::GaveUp(format!("the mqtt reconnection gave up: {err}")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_pause_doubles_from_the_first_up_to_the_longest() {
        let settings =
            AutoReconnect::new().pauses(Duration::from_millis(300), Duration::from_secs(1));

        let mut pause = settings.first();
        let mut pauses = vec![pause];
        for _ in 0..3 {
            pause = settings.next(pause);
            pauses.push(pause);
        }

        assert_eq!(
            pauses,
            [300, 600, 1000, 1000].map(Duration::from_millis).to_vec()
        );
    }

    #[test]
    fn a_first_pause_longer_than_the_longest_pauses_the_longest() {
        let settings = AutoReconnect::new().pauses(Duration::from_secs(9), Duration::from_secs(2));

        assert_eq!(settings.first(), Duration::from_secs(2));
    }

    #[test]
    fn a_server_disconnect_names_its_reason_code() {
        use rumqttc::v5::mqttbytes::v5::DisconnectReasonCode;

        let announced =
            LostConnection::new(&ConnectionError::MqttState(StateError::ServerDisconnect {
                reason_code: DisconnectReasonCode::ServerShuttingDown,
                reason_string: None,
            }));
        let dropped = LostConnection::new(&ConnectionError::MqttState(StateError::AwaitPingResp));

        assert_eq!(announced.server_reason_code(), Some(0x8B));
        assert_eq!(dropped.server_reason_code(), None);
    }
}
