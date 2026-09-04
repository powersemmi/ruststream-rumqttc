//! [`MqttTestSubscriber`] and [`MqttTestMessage`].

use std::future::{Future, ready};
use std::num::NonZeroUsize;
use std::sync::{Arc, OnceLock};

use futures::Stream;

use ruststream::{
    AckError, BatchSubscriber, BufferedSubscriber, HeaderMap, IncomingMessage, Subscriber,
    testing::Coordinator,
};

use crate::error::MqttError;
use crate::subscriber::PAGE_MAX_WAIT;
use crate::testing::broker::TestState;
use crate::testing::router::{Delivery, DeliveryReceiver, DeliverySender, SubscriptionId};

/// The in-process counterpart of the real subscriber's wire half: one delivery at a time off
/// the router's channel.
struct WireTestSubscriber {
    state: Arc<TestState>,
    id: SubscriptionId,
    rx: DeliveryReceiver,
    requeue: DeliverySender,
    /// A clone of the broker's harness coordinator, threaded into each yielded message so a
    /// requeue re-counts and a consumed delivery decrements. `None` outside a harness run.
    coordinator: Option<Coordinator>,
}

impl std::fmt::Debug for WireTestSubscriber {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WireTestSubscriber").finish_non_exhaustive()
    }
}

impl Drop for WireTestSubscriber {
    fn drop(&mut self) {
        self.state.router.unsubscribe(self.id);
    }
}

impl Subscriber for WireTestSubscriber {
    type Message = MqttTestMessage;
    type Error = MqttError;

    fn stream(&mut self) -> impl Stream<Item = Result<Self::Message, Self::Error>> + Send + '_ {
        let requeue = self.requeue.clone();
        let coordinator = self.coordinator.clone();
        // Poll the receiver in place rather than wrapping it in an owning stream, so `stream`
        // can be called again after the returned stream is dropped (the runtime and the
        // conformance helpers re-enter it per call).
        futures::stream::poll_fn(move |cx| {
            self.rx.poll_recv(cx).map(|next| {
                next.map(|delivery| {
                    Ok(MqttTestMessage::new(
                        delivery,
                        requeue.clone(),
                        coordinator.clone(),
                    ))
                })
            })
        })
    }
}

/// Subscriber returned by [`ConnectedMqttTestBroker`](crate::testing::ConnectedMqttTestBroker).
///
/// Dropping it unregisters the subscription, so handlers stop receiving as soon as their task
/// finishes. Pages are assembled on the client with the real subscriber's deadline, so a page
/// handler behaves under the harness the way it behaves on a server.
pub struct MqttTestSubscriber {
    paged: BufferedSubscriber<WireTestSubscriber>,
}

impl std::fmt::Debug for MqttTestSubscriber {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MqttTestSubscriber").finish_non_exhaustive()
    }
}

impl MqttTestSubscriber {
    pub(crate) fn new(
        state: Arc<TestState>,
        id: SubscriptionId,
        rx: DeliveryReceiver,
        requeue: DeliverySender,
        coordinator: Option<Coordinator>,
    ) -> Self {
        Self {
            paged: BufferedSubscriber::new(WireTestSubscriber {
                state,
                id,
                rx,
                requeue,
                coordinator,
            })
            .max_wait(PAGE_MAX_WAIT),
        }
    }
}

impl Subscriber for MqttTestSubscriber {
    type Message = MqttTestMessage;
    type Error = MqttError;

    fn stream(&mut self) -> impl Stream<Item = Result<Self::Message, Self::Error>> + Send + '_ {
        self.paged.stream()
    }
}

impl BatchSubscriber for MqttTestSubscriber {
    type Batch = Vec<MqttTestMessage>;

    fn batches(
        &mut self,
        size: NonZeroUsize,
    ) -> impl Stream<Item = Result<Self::Batch, MqttError>> + Send + '_ {
        self.paged.batches(size)
    }
}

/// Message handed to handlers from an [`MqttTestSubscriber`].
///
/// `ack` consumes the handle; `nack(requeue = true)` re-queues the delivery on the owning
/// subscription's channel so the next handler invocation sees it again; `nack(requeue = false)`
/// drops it, matching the real subscriber's reject path in effect.
pub struct MqttTestMessage {
    delivery: Option<Delivery>,
    requeue: DeliverySender,
    /// A clone of the broker's harness coordinator. When set, this delivery is counted in
    /// flight and is decremented exactly once when the message is consumed or dropped.
    coordinator: Option<Coordinator>,
}

impl Drop for MqttTestMessage {
    /// Counts this delivery consumed exactly once: on ack, nack, or an unsettled drop. A
    /// requeue re-enqueues a fresh delivery first, so the in-flight count stays balanced.
    fn drop(&mut self) {
        if let Some(coordinator) = &self.coordinator {
            coordinator.consumed();
        }
    }
}

impl std::fmt::Debug for MqttTestMessage {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MqttTestMessage").finish_non_exhaustive()
    }
}

impl MqttTestMessage {
    pub(crate) fn new(
        delivery: Delivery,
        requeue: DeliverySender,
        coordinator: Option<Coordinator>,
    ) -> Self {
        Self {
            delivery: Some(delivery),
            requeue,
            coordinator,
        }
    }
}

impl IncomingMessage for MqttTestMessage {
    fn payload(&self) -> &[u8] {
        self.delivery
            .as_ref()
            .map(|d| d.payload.as_ref())
            .unwrap_or_default()
    }

    fn headers(&self) -> &HeaderMap {
        static EMPTY: OnceLock<HeaderMap> = OnceLock::new();
        self.delivery
            .as_ref()
            .map_or_else(|| EMPTY.get_or_init(HeaderMap::new), |d| &d.headers)
    }

    fn ack(mut self) -> impl Future<Output = Result<(), AckError>> {
        self.delivery.take();
        ready(Ok(()))
    }

    fn nack(mut self, requeue: bool) -> impl Future<Output = Result<(), AckError>> {
        let delivery = self
            .delivery
            .take()
            .expect("MqttTestMessage ack/nack invoked twice");
        if requeue {
            let sent = self.requeue.send(delivery);
            // The requeue bypasses fanout, so count the re-enqueue here to balance this
            // message's `Drop` decrement. The redelivered copy is consumed in turn.
            if sent.is_ok()
                && let Some(coordinator) = &self.coordinator
            {
                coordinator.enqueued();
            }
        }
        ready(Ok(()))
    }
}
