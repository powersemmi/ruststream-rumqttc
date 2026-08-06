//! [`MqttSubscriber`]: a stream of deliveries fed by the connection task.

use std::sync::Arc;

use futures::Stream;
use rumqttc::v5::AsyncClient;
use ruststream::Subscriber;
use tokio::sync::mpsc;

use crate::conn::Shared;
use crate::error::MqttError;
use crate::message::MqttMessage;

/// A subscription to one MQTT topic filter; yields [`MqttMessage`]s.
///
/// Delivery back-pressure is the protocol's receive-maximum: the broker bounds unacked
/// `QoS` 1/2 deliveries, so unsettled messages cap what sits in this subscriber's queue
/// (`QoS` 0 has no such bound by design). Dropping the subscriber unsubscribes the filter.
pub struct MqttSubscriber {
    filter: String,
    id: u64,
    shared: Arc<Shared>,
    client: AsyncClient,
    rx: mpsc::UnboundedReceiver<Result<MqttMessage, MqttError>>,
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
            id,
            shared,
            client,
            rx,
        }
    }

    /// The plain topic filter this subscription matches.
    #[must_use]
    pub fn filter(&self) -> &str {
        &self.filter
    }
}

impl Drop for MqttSubscriber {
    fn drop(&mut self) {
        if let Some(wire_filter) = self.shared.remove(self.id) {
            let _ = self.client.try_unsubscribe(wire_filter);
        }
    }
}

impl Subscriber for MqttSubscriber {
    type Message = MqttMessage;
    type Error = MqttError;

    fn stream(&mut self) -> impl Stream<Item = Result<MqttMessage, MqttError>> + Send + '_ {
        // Poll the channel in place rather than wrapping it in an owning stream, so `stream`
        // can be called again after the returned stream is dropped (the runtime and the
        // conformance helpers re-enter it per call).
        futures::stream::poll_fn(move |cx| self.rx.poll_recv(cx))
    }
}
