//! [`MqttSubscriber`]: a stream of deliveries fed by the connection task.

use std::num::NonZeroUsize;
use std::sync::Arc;
use std::time::Duration;

use futures::Stream;
use rumqttc::v5::AsyncClient;
use ruststream::{BatchSubscriber, BufferedSubscriber, Subscriber};
use tokio::sync::mpsc;

use crate::conn::Shared;
use crate::error::MqttError;
use crate::message::MqttMessage;

/// How long a partial batch waits for more deliveries after its first one.
///
/// The crate's own choice, not the mount site's: MQTT delivers one PUBLISH packet at a time over
/// a network event loop, so a batch that is not yet full waits a network-shaped moment for the
/// next packet rather than an in-process one. The batch size is the registration's, and arrives
/// per subscription as the argument of [`BatchSubscriber::batches`].
///
/// The in-process test broker batches with the same deadline, so a batch handler behaves the same
/// under the harness as it does on a server.
pub(crate) const BATCH_MAX_WAIT: Duration = Duration::from_millis(20);

/// The wire half of a subscription: whatever the connection task demultiplexed onto this
/// filter's channel, one delivery at a time, which is all the protocol offers.
struct WireSubscriber {
    id: u64,
    shared: Arc<Shared>,
    client: AsyncClient,
    rx: mpsc::UnboundedReceiver<Result<MqttMessage, MqttError>>,
}

impl std::fmt::Debug for WireSubscriber {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WireSubscriber").finish_non_exhaustive()
    }
}

impl Drop for WireSubscriber {
    fn drop(&mut self) {
        if let Some(wire_filter) = self.shared.remove(self.id) {
            let _ = self.client.try_unsubscribe(wire_filter);
        }
    }
}

impl Subscriber for WireSubscriber {
    type Message = MqttMessage;
    type Error = MqttError;

    fn stream(&mut self) -> impl Stream<Item = Result<MqttMessage, MqttError>> + Send + '_ {
        // Poll the channel in place rather than wrapping it in an owning stream, so `stream`
        // can be called again after the returned stream is dropped (the runtime and the
        // conformance helpers re-enter it per call).
        futures::stream::poll_fn(move |cx| self.rx.poll_recv(cx))
    }
}

/// A subscription to one MQTT topic filter; yields [`MqttMessage`]s one at a time, or in batches.
///
/// Delivery back-pressure is the protocol's receive-maximum: the broker bounds unacked
/// `QoS` 1/2 deliveries, so unsettled messages cap what sits in this subscriber's queue
/// (`QoS` 0 has no such bound by design). Dropping the subscriber unsubscribes the filter.
///
/// MQTT has no batch fetch - a PUBLISH packet carries one message - so the batches a `&[T]`
/// handler consumes are assembled on the client by the framework's
/// [`BufferedSubscriber`]: a batch closes when it holds the size the mount site named, or once
/// the crate's own 20 ms deadline has elapsed after its first delivery. Nothing at the mount
/// site says which of the two happened, which is the point.
pub struct MqttSubscriber {
    filter: String,
    buffered: BufferedSubscriber<WireSubscriber>,
}

impl std::fmt::Debug for MqttSubscriber {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MqttSubscriber")
            .field("filter", &self.filter)
            .finish_non_exhaustive()
    }
}

impl MqttSubscriber {
    pub(crate) fn new(
        filter: String,
        id: u64,
        shared: Arc<Shared>,
        client: AsyncClient,
        rx: mpsc::UnboundedReceiver<Result<MqttMessage, MqttError>>,
    ) -> Self {
        Self {
            filter,
            buffered: BufferedSubscriber::new(WireSubscriber {
                id,
                shared,
                client,
                rx,
            })
            .max_wait(BATCH_MAX_WAIT),
        }
    }

    /// The plain topic filter this subscription matches.
    #[must_use]
    pub fn filter(&self) -> &str {
        &self.filter
    }
}

impl Subscriber for MqttSubscriber {
    type Message = MqttMessage;
    type Error = MqttError;

    fn stream(&mut self) -> impl Stream<Item = Result<MqttMessage, MqttError>> + Send + '_ {
        self.buffered.stream()
    }
}

impl BatchSubscriber for MqttSubscriber {
    type Batch = Vec<MqttMessage>;

    /// # Cancel safety
    ///
    /// Cancel-safe between polls, like [`Subscriber::stream`]: the batch being filled lives
    /// inside the returned stream and survives a cancelled poll. Dropping the whole stream
    /// abandons that batch's deliveries unacknowledged, and MQTT redelivers them when a
    /// persistent session resumes.
    fn batches(
        &mut self,
        size: NonZeroUsize,
    ) -> impl Stream<Item = Result<Self::Batch, MqttError>> + Send + '_ {
        self.buffered.batches(size)
    }
}
