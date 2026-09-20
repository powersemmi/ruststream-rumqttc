//! [`MqttTestBroker`]: the in-process transport and its connected form.

use std::future::{Future, ready};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, OnceLock};

use bytes::Bytes;
use ruststream::testing::{Coordinator, TestableBroker};
use ruststream::{
    AddressedCopies, Broker, BytesMut, ConnectedBroker, DefaultPublish, OutgoingMessage, Publisher,
    RawMessage, Subscribe, Take,
};

use crate::error::MqttError;
use crate::filter::{MqttFilter, MqttTopic, Qos};
use crate::publisher::{MqttPublish, MqttPublishOptions};
use crate::testing::router::AddressRouter;
use crate::testing::subscriber::MqttTestSubscriber;

/// Shared state of one in-process broker: the router plus the harness coordinator.
#[derive(Debug, Default)]
pub(crate) struct TestState {
    pub(crate) router: AddressRouter,
    coordinator: OnceLock<Coordinator>,
    closed: AtomicBool,
}

impl TestState {
    fn coordinator(&self) -> Option<&Coordinator> {
        self.coordinator.get()
    }

    /// The shutdown witness closes the connection for its owner, but handles handed out earlier
    /// alias it and outlive it, so they ask here - as the real handles ask the connection task.
    fn ensure_open(&self) -> Result<(), MqttError> {
        if self.closed.load(Ordering::Acquire) {
            return Err(MqttError::NotConnected);
        }
        Ok(())
    }

    pub(crate) fn publish(
        &self,
        name: &str,
        payload: Bytes,
        headers: ruststream::HeaderMap,
        qos: Qos,
    ) {
        self.router
            .publish(name, payload, headers, qos, self.coordinator());
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
            qos: Qos::default(),
            retain: false,
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
    /// A publisher from the connected form, with the same policy defaults as the real one.
    #[must_use]
    pub fn publisher(&self) -> MqttTestPublisher {
        MqttTestPublisher {
            state: Arc::clone(&self.state),
            qos: Qos::default(),
            retain: false,
        }
    }

    /// A publisher carrying `policy`, mirroring
    /// [`ConnectedMqttBroker::publisher_with`](crate::ConnectedMqttBroker). Both defaults travel,
    /// because a call that names neither has to resolve against the same values here as on the
    /// wire; what the resolved retain flag then does is the protocol's, and stops here.
    #[must_use]
    pub(crate) fn publisher_with(&self, policy: MqttPublish) -> MqttTestPublisher {
        MqttTestPublisher {
            state: Arc::clone(&self.state),
            qos: policy.qos_value(),
            retain: policy.retain_value(),
        }
    }

    /// Opens a subscription for `topic`, mirroring
    /// [`ConnectedMqttBroker::subscribe_topic`](crate::ConnectedMqttBroker::subscribe_topic): the
    /// descriptor is validated first, its filter selects the deliveries, its share group makes the
    /// subscription a competing consumer, and its quality of service decides whether a delivery
    /// can be settled at all.
    ///
    /// # Errors
    ///
    /// Returns [`MqttError::Invalid`] for a descriptor no broker would accept, and
    /// [`MqttError::NotConnected`] once this broker has shut down.
    pub fn subscribe_topic(
        &self,
        topic: MqttTopic,
    ) -> impl Future<Output = Result<MqttTestSubscriber, MqttError>> {
        // Registering is a lock and a channel, so there is nothing to await here. The signature
        // stays the real one's, which waits for the broker's SUBACK.
        ready(
            topic
                .validate()
                .and_then(|()| self.register(topic.into_filter())),
        )
    }

    /// Opens a subscription for `filter`, mirroring
    /// [`ConnectedMqttBroker::subscribe_filter`](crate::ConnectedMqttBroker::subscribe_filter).
    ///
    /// # Errors
    ///
    /// Returns [`MqttError::Invalid`] for a descriptor no broker would accept, and
    /// [`MqttError::NotConnected`] once this broker has shut down.
    pub fn subscribe_filter(
        &self,
        filter: MqttFilter,
    ) -> impl Future<Output = Result<MqttTestSubscriber, MqttError>> {
        ready(filter.validate().and_then(|()| self.register(filter)))
    }

    fn register(&self, topic: MqttFilter) -> Result<MqttTestSubscriber, MqttError> {
        self.state.ensure_open()?;
        let (filter, group, qos) = topic.into_parts();
        let (id, rx) = self.state.router.subscribe(filter, group);
        Ok(MqttTestSubscriber::new(
            Arc::clone(&self.state),
            id,
            rx,
            qos,
            self.state.coordinator().cloned(),
        ))
    }
}

impl ConnectedBroker for ConnectedMqttTestBroker {
    type Error = MqttError;
    type Closed = ();

    fn shutdown(self) -> impl Future<Output = Result<(), Self::Error>> {
        // Closed before the registry is dropped, so a handle racing the shutdown is refused
        // rather than routed into a registry that is about to go.
        self.state.closed.store(true, Ordering::Release);
        self.state.router.clear();
        ready(Ok(()))
    }
}

impl Subscribe for ConnectedMqttTestBroker {
    type Subscriber = MqttTestSubscriber;
    /// The real broker's answer, so a registration composes the same way here.
    type Copies = AddressedCopies;

    fn subscribe(&self, name: &str) -> impl Future<Output = Result<Self::Subscriber, Self::Error>> {
        self.subscribe_topic(MqttTopic::new(name))
    }
}

impl TestableBroker for ConnectedMqttTestBroker {
    fn install_coordinator(&self, coordinator: Coordinator) {
        let _ = self.state.coordinator.set(coordinator);
    }

    fn inject(&self, message: OutgoingMessage<'_>) {
        // An injection stands in for an outside producer this crate did not configure, so it
        // publishes at the default quality of service rather than at none: a harness message must
        // be settleable, as one from any ordinary client would be.
        self.state.publish(
            message.name(),
            Bytes::copy_from_slice(message.payload()),
            message.headers().clone(),
            Qos::default(),
        );
    }

    fn published(&self, name: &str) -> Vec<RawMessage> {
        self.state.router.published(name)
    }
}

ruststream::register_testable_broker!(ConnectedMqttTestBroker);

/// Publisher for the in-process broker: what [`MqttPublish`](crate::MqttPublish) pairs into here,
/// and what [`publisher`](ConnectedMqttTestBroker::publisher) hands out directly.
///
/// It declares the crate's own [`MqttPublishOptions`], so a handler bound to
/// `Out<impl Publisher<Options = MqttPublishOptions>, Marker>` mounts here as it mounts on the
/// real broker, and the steps a body takes are the same steps.
#[derive(Debug, Clone)]
pub struct MqttTestPublisher {
    state: Arc<TestState>,
    qos: Qos,
    retain: bool,
}

impl Publisher for MqttTestPublisher {
    /// The same answer the real publisher gives: the recorded delivery keeps its payload, as a
    /// queued PUBLISH packet keeps it.
    type Payload = Take;

    type Error = MqttError;
    type Options = MqttPublishOptions;

    fn publish(
        &self,
        msg: OutgoingMessage<'_, BytesMut>,
        options: Option<&Self::Options>,
    ) -> impl Future<Output = Result<(), Self::Error>> {
        // The call's arguments are resolved over the policy's here exactly as the real publisher
        // resolves them, so the quality of service a delivery is settled under is the one a
        // server would have delivered it at. The retain flag resolves and stops: keeping a last
        // message per topic is protocol behaviour this transport does not reproduce, and the live
        // suite is what covers it.
        let (qos, _retain) = MqttPublishOptions::resolve(options, self.qos, self.retain);
        let outcome = self.state.ensure_open().map(|()| {
            let headers = msg.headers().clone();
            let name = msg.name();
            self.state
                .publish(name, msg.into_payload().freeze(), headers, qos);
        });
        ready(outcome)
    }
}

// The policy a service declares is the one the runtime pairs here too (the impl lives next to the
// real one, in `publisher`), so a `publish("dest")` handler mounted without an explicit publisher
// gets its reply publisher from the same type on both brokers.
impl DefaultPublish for ConnectedMqttTestBroker {
    type Policy = MqttPublish;
}
