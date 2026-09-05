//! [`MqttTestBroker`]: the in-process transport and its connected form.

use std::future::{Future, ready};
use std::sync::{Arc, OnceLock};

use bytes::Bytes;
use ruststream::testing::{Coordinator, TestableBroker};
use ruststream::{
    Broker, ConnectedBroker, DefaultPublish, OutgoingMessage, PairError, PublishPolicy, Publisher,
    RawMessage, Subscribe,
};

use crate::error::MqttError;
use crate::message::without_per_message;
use crate::publisher::MqttPublishOptions;
use crate::testing::router::AddressRouter;
use crate::testing::subscriber::MqttTestSubscriber;

/// Shared state of one in-process broker: the router plus the harness coordinator.
#[derive(Debug, Default)]
pub(crate) struct TestState {
    pub(crate) router: AddressRouter,
    coordinator: OnceLock<Coordinator>,
}

impl TestState {
    fn coordinator(&self) -> Option<&Coordinator> {
        self.coordinator.get()
    }

    pub(crate) fn publish(&self, name: &str, payload: Bytes, headers: ruststream::HeaderMap) {
        self.router
            .publish(name, payload, headers, self.coordinator());
    }
}

/// An in-process stand-in for [`MqttBroker`](crate::MqttBroker): same core routing, no server.
///
/// # Examples
///
/// ```
/// use ruststream_rumqttc::testing::MqttTestBroker;
///
/// let broker = MqttTestBroker::new();
/// # let _ = broker;
/// ```
#[derive(Debug, Clone, Default)]
#[must_use]
pub struct MqttTestBroker {
    state: Arc<TestState>,
}

impl MqttTestBroker {
    /// Creates an empty in-process broker. Synchronous and I/O-free, like the real `new`.
    pub fn new() -> Self {
        Self::default()
    }

    /// A publisher usable before `connect`, mirroring the real broker's early-publisher path.
    #[must_use]
    pub fn publisher(&self) -> MqttTestPublisher {
        MqttTestPublisher {
            state: Arc::clone(&self.state),
        }
    }
}

impl Broker for MqttTestBroker {
    type Error = MqttError;
    type Connected = ConnectedMqttTestBroker;

    fn connect(self) -> impl Future<Output = Result<Self::Connected, Self::Error>> {
        ready(Ok(ConnectedMqttTestBroker { state: self.state }))
    }
}

/// The connected form of [`MqttTestBroker`]; implements
/// [`TestableBroker`](ruststream::testing::TestableBroker) for the harness and the conformance
/// suite.
#[derive(Debug, Clone)]
pub struct ConnectedMqttTestBroker {
    state: Arc<TestState>,
}

impl ConnectedMqttTestBroker {
    /// A publisher from the connected form.
    #[must_use]
    pub fn publisher(&self) -> MqttTestPublisher {
        MqttTestPublisher {
            state: Arc::clone(&self.state),
        }
    }
}

impl ConnectedBroker for ConnectedMqttTestBroker {
    type Error = MqttError;
    type Closed = ();

    fn shutdown(self) -> impl Future<Output = Result<(), Self::Error>> {
        self.state.router.clear();
        ready(Ok(()))
    }
}

impl Subscribe for ConnectedMqttTestBroker {
    type Subscriber = MqttTestSubscriber;

    fn subscribe(&self, name: &str) -> impl Future<Output = Result<Self::Subscriber, Self::Error>> {
        let (id, requeue, rx) = self.state.router.subscribe(name.to_owned());
        ready(Ok(MqttTestSubscriber::new(
            Arc::clone(&self.state),
            id,
            rx,
            requeue,
            self.state.coordinator().cloned(),
        )))
    }
}

impl TestableBroker for ConnectedMqttTestBroker {
    fn install_coordinator(&self, coordinator: Coordinator) {
        let _ = self.state.coordinator.set(coordinator);
    }

    fn inject(&self, message: OutgoingMessage<'_>) {
        self.state.publish(
            message.name(),
            Bytes::copy_from_slice(message.payload()),
            message.headers().clone(),
        );
    }

    fn published(&self, name: &str) -> Vec<RawMessage> {
        self.state.router.published(name)
    }
}

ruststream::register_testable_broker!(ConnectedMqttTestBroker);

/// Publisher for the in-process broker.
#[derive(Debug, Clone)]
pub struct MqttTestPublisher {
    state: Arc<TestState>,
}

impl Publisher for MqttTestPublisher {
    type Error = MqttError;

    fn publish(&self, msg: OutgoingMessage<'_>) -> impl Future<Output = Result<(), Self::Error>> {
        // The per-message arguments are consumed here as the real publisher consumes them, so a
        // delivery carries what a subscriber would see and an unreadable one is refused on the
        // same terms. Applying them is protocol behaviour this transport does not reproduce,
        // which is what the live suite covers.
        let outcome = without_per_message(msg.headers().clone()).map(|headers| {
            self.state
                .publish(msg.name(), Bytes::copy_from_slice(msg.payload()), headers);
        });
        ready(outcome)
    }
}

// The same steps on the in-process transport, so a handler bound to them mounts on both brokers.
impl MqttPublishOptions for MqttTestPublisher {}

/// The publish policy for [`MqttTestPublisher`], mirroring
/// [`MqttPublish`](crate::MqttPublish) on the real broker.
///
/// # Examples
///
/// ```
/// use ruststream_rumqttc::testing::MqttTestPublish;
///
/// let policy = MqttTestPublish::default();
/// # let _ = policy;
/// ```
#[derive(Debug, Clone, Copy, Default)]
#[must_use]
pub struct MqttTestPublish;

impl PublishPolicy<ConnectedMqttTestBroker> for MqttTestPublish {
    type Live = MqttTestPublisher;

    fn pair(
        self,
        connected: &ConnectedMqttTestBroker,
    ) -> impl Future<Output = Result<Self::Live, PairError>> {
        ready(Ok(connected.publisher()))
    }
}

impl DefaultPublish for ConnectedMqttTestBroker {
    type Policy = MqttTestPublish;
}
