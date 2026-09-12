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
    /// consumed delivery decrements the in-flight count. `None` outside a harness run.
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
/// Settlement answers what the wire answers, and the delivered quality of service - the lesser of
/// the publish's and the subscription's - decides what that is. A delivery that comes out at
/// [`Qos::AtMostOnce`] carries no acknowledgement, so `ack` and `nack` both report
/// [`AckError::Unsupported`] exactly as the real message does. Above that level `ack` consumes the
/// handle, `nack(requeue = false)` acknowledges because dropping is the only terminal outcome MQTT
/// offers, and `nack(requeue = true)` reports [`AckError::Unsupported`] without redelivering: MQTT
/// has no negative acknowledgement, and a stand-in that redelivered would let a service prove a
/// retry it never gets on a wire.
pub struct MqttTestMessage {
    delivery: Option<Delivery>,
    /// Mirrors the real message's acker: absent for `QoS` 0, where nothing can be settled.
    acknowledges: bool,
    /// A clone of the broker's harness coordinator. When set, this delivery is counted in
    /// flight and is decremented exactly once when the message is consumed or dropped.
    coordinator: Option<Coordinator>,
}

impl Drop for MqttTestMessage {
    /// Counts this delivery consumed exactly once: on ack, nack, or an unsettled drop. No
    /// settlement puts one back, so nothing has to balance the count.
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
        if !self.acknowledges {
            return ready(Err(AckError::Unsupported));
        }
        if requeue {
            // MQTT has no negative acknowledgement, so the real message reports this and the
            // delivery stays unacknowledged until the session resumes. A stand-in that
            // redelivered instead would let a service prove a retry it does not get on a wire.
            return ready(Err(AckError::Unsupported));
        }
        ready(Ok(()))
    }
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;
    use rumqttc::v5::mqttbytes::QoS;
    use rumqttc::v5::mqttbytes::v5::Publish;
    use rumqttc::v5::{AsyncClient, MqttOptions};

    use super::*;
    use crate::message::MqttMessage;

    /// A delivery at the level the wire counterpart below carries, so both sides of the
    /// comparison are settleable and the answers being compared are the settlement's own.
    fn stand_in() -> MqttTestMessage {
        MqttTestMessage::new(
            Delivery {
                payload: Bytes::from_static(b"{}"),
                headers: HeaderMap::new(),
                qos: Qos::AtLeastOnce,
            },
            true,
            None,
        )
    }

    /// The live counterpart of what the stand-in yields: a `QoS` 1 delivery holding an acker.
    /// The client is never connected, which is enough - `nack` answers before reaching it, and
    /// `ack` only has to hand its request to a live event loop.
    fn on_the_wire() -> (MqttMessage, rumqttc::v5::EventLoop) {
        let (client, eventloop) =
            AsyncClient::new(MqttOptions::new("settlement", "localhost", 1883), 8);
        let mut publish = Publish::new("orders", QoS::AtLeastOnce, b"{}".as_slice(), None);
        publish.pkid = 1;
        (
            MqttMessage::new("orders".to_owned(), &publish, Some(client)),
            eventloop,
        )
    }

    /// The one place both transports are read together. A stand-in that answers differently
    /// from the wire lets a green test stand for behaviour a service never gets.
    #[tokio::test]
    async fn the_stand_in_settles_the_way_the_wire_does() {
        let (live, _eventloop) = on_the_wire();
        assert!(
            matches!(live.nack(true).await, Err(AckError::Unsupported)),
            "the wire has no negative acknowledgement"
        );
        assert!(matches!(
            stand_in().nack(true).await,
            Err(AckError::Unsupported)
        ));

        let (live, _eventloop) = on_the_wire();
        assert!(live.nack(false).await.is_ok(), "dropping is an acknowledge");
        assert!(stand_in().nack(false).await.is_ok());

        let (live, _eventloop) = on_the_wire();
        assert!(live.ack().await.is_ok());
        assert!(stand_in().ack().await.is_ok());
    }
}
