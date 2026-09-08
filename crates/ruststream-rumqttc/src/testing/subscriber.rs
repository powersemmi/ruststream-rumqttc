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
use crate::filter::Qos;
use crate::subscriber::BATCH_MAX_WAIT;
use crate::testing::broker::TestState;
use crate::testing::router::{Delivery, DeliveryReceiver, SubscriptionId};

/// The in-process counterpart of the real subscriber's wire half: one delivery at a time off
/// the router's channel.
struct WireTestSubscriber {
    state: Arc<TestState>,
    id: SubscriptionId,
    rx: DeliveryReceiver,
    /// The quality of service this subscription was opened with. It caps the delivery's own, the
    /// way a server delivers at the lesser of the two, and a delivery that comes out at `QoS` 0
    /// carries no acknowledgement.
    qos: Qos,
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
        let subscribed_at = self.qos;
        let coordinator = self.coordinator.clone();
        // Poll the receiver in place rather than wrapping it in an owning stream, so `stream`
        // can be called again after the returned stream is dropped (the runtime and the
        // conformance helpers re-enter it per call).
        futures::stream::poll_fn(move |cx| {
            self.rx.poll_recv(cx).map(|next| {
                next.map(|delivery| {
                    // A delivery comes out at the lesser of the two levels, as it does on the
                    // wire, so an acknowledgement needs both sides to carry one.
                    let acknowledges =
                        delivery.qos != Qos::AtMostOnce && subscribed_at != Qos::AtMostOnce;
                    Ok(MqttTestMessage::new(
                        delivery,
                        acknowledges,
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
/// finishes. Batches are assembled on the client with the real subscriber's deadline, so a batch
/// handler behaves under the harness the way it behaves on a server.
pub struct MqttTestSubscriber {
    buffered: BufferedSubscriber<WireTestSubscriber>,
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
        qos: Qos,
        coordinator: Option<Coordinator>,
    ) -> Self {
        Self {
            buffered: BufferedSubscriber::new(WireTestSubscriber {
                state,
                id,
                rx,
                qos,
                coordinator,
            })
            .max_wait(BATCH_MAX_WAIT),
        }
    }
}

impl Subscriber for MqttTestSubscriber {
    type Message = MqttTestMessage;
    type Error = MqttError;

    fn stream(&mut self) -> impl Stream<Item = Result<Self::Message, Self::Error>> + Send + '_ {
        self.buffered.stream()
    }
}

impl BatchSubscriber for MqttTestSubscriber {
    type Batch = Vec<MqttTestMessage>;

    fn batches(
        &mut self,
        size: NonZeroUsize,
    ) -> impl Stream<Item = Result<Self::Batch, MqttError>> + Send + '_ {
        self.buffered.batches(size)
    }
}

/// Message handed to handlers from an [`MqttTestSubscriber`].
///
/// Settlement is the real message's, answer for answer. It follows the delivered quality of
/// service - the lesser of the publish's and the subscription's, as on the wire - so a delivery
/// that comes out at [`Qos::AtMostOnce`] carries no acknowledgement and reports
/// [`AckError::Unsupported`]. Otherwise `ack` acknowledges and `nack(requeue = false)`
/// acknowledges too, dropping being the protocol's only terminal answer.
///
/// `nack(requeue = true)` reports [`AckError::Unsupported`] whatever the quality of service,
/// because MQTT has no negative acknowledgement: a delivery a handler asks to have redelivered is
/// simply left unacknowledged, and comes back when a persistent session resumes. A handler
/// returning `HandlerOutcome::retry()` therefore does here what it does against a server -
/// nothing, inside this connection - so a test cannot see a retry the production service will not
/// perform.
pub struct MqttTestMessage {
    delivery: Option<Delivery>,
    /// Mirrors the real message's acker: absent for `QoS` 0, where nothing can be settled.
    acknowledges: bool,
    /// A clone of the broker's harness coordinator. When set, this delivery is counted in
    /// flight and is decremented exactly once when the message is consumed or dropped.
    coordinator: Option<Coordinator>,
}

impl Drop for MqttTestMessage {
    /// Counts this delivery consumed exactly once: on ack, nack, or an unsettled drop. Nothing
    /// re-enqueues, so every delivery is counted in and out exactly once.
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
        acknowledges: bool,
        coordinator: Option<Coordinator>,
    ) -> Self {
        Self {
            delivery: Some(delivery),
            acknowledges,
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
        // The handle is consumed either way, as it is on the wire: what QoS 0 lacks is the
        // acknowledgement, not the delivery.
        self.delivery.take();
        ready(if self.acknowledges {
            Ok(())
        } else {
            Err(AckError::Unsupported)
        })
    }

    fn nack(mut self, requeue: bool) -> impl Future<Output = Result<(), AckError>> {
        self.delivery
            .take()
            .expect("MqttTestMessage ack/nack invoked twice");
        // Asking for redelivery is the one thing the protocol cannot express, so refusing is the
        // whole answer: there is nothing to re-enqueue, exactly as there is nothing to send.
        ready(if requeue || !self.acknowledges {
            Err(AckError::Unsupported)
        } else {
            Ok(())
        })
    }
}
