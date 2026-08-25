//! [`MqttPublisher`] and its [`MqttPublish`] policy.

use std::future::{Future, ready};

use bytes::Bytes;
use rumqttc::v5::mqttbytes::valid_topic;
use ruststream::{OutgoingMessage, PairError, PublishPolicy, Publisher};

use crate::broker::{ConnectedMqttBroker, CoreCell};
use crate::error::MqttError;
use crate::filter::Qos;
use crate::message::to_publish_properties;

/// Publishes messages to MQTT topics through the shared connection.
///
/// The publish is queued into the client session: for `QoS` 1/2 the session's state machine
/// retransmits until the broker acknowledges (surviving reconnects), so `Ok` means "owned by
/// the session", not "broker confirmed". Buildable before `connect` and usable until
/// `shutdown`; afterwards every publish reports [`MqttError::NotConnected`].
#[derive(Clone)]
pub struct MqttPublisher {
    cell: CoreCell,
    qos: Qos,
    retain: bool,
}

impl std::fmt::Debug for MqttPublisher {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MqttPublisher")
            .field("qos", &self.qos)
            .field("retain", &self.retain)
            .finish_non_exhaustive()
    }
}

impl MqttPublisher {
    pub(crate) fn new(cell: CoreCell, qos: Qos, retain: bool) -> Self {
        Self { cell, qos, retain }
    }
}

impl Publisher for MqttPublisher {
    type Error = MqttError;

    async fn publish(&self, msg: OutgoingMessage<'_>) -> Result<(), Self::Error> {
        let core = self.cell.get().ok_or(MqttError::NotConnected)?;
        core.shared.ensure_open()?;
        // The client's send-path error cannot say why a request failed, so the topic is
        // validated here; a remaining failure unambiguously means the connection is gone.
        if !valid_topic(msg.name()) {
            return Err(MqttError::Publish {
                topic: msg.name().to_owned(),
                reason: "not a valid MQTT topic (wildcards are subscribe-only)".to_owned(),
            });
        }
        let payload = Bytes::copy_from_slice(msg.payload());
        let outcome = match to_publish_properties(&msg) {
            Some(properties) => {
                core.client
                    .publish_bytes_with_properties(
                        msg.name(),
                        self.qos.to_client(),
                        self.retain,
                        payload,
                        properties,
                    )
                    .await
            }
            None => {
                core.client
                    .publish_bytes(msg.name(), self.qos.to_client(), self.retain, payload)
                    .await
            }
        };
        outcome.map_err(|_| MqttError::Publish {
            topic: msg.name().to_owned(),
            reason: "the mqtt connection task has shut down".to_owned(),
        })
    }
}

/// The publish policy for [`MqttPublisher`]: quality of service and the retain flag as pure
/// declaration, paired with the connected broker by the runtime after `connect`.
///
/// # Examples
///
/// ```
/// use ruststream_rumqttc::{MqttPublish, Qos};
///
/// let policy = MqttPublish::default().qos(Qos::ExactlyOnce).retain(true);
/// # let _ = policy;
/// ```
#[derive(Debug, Clone, Copy, Default)]
#[must_use]
pub struct MqttPublish {
    qos: Qos,
    retain: bool,
}

impl MqttPublish {
    /// Sets the delivery quality of service. Defaults to [`Qos::AtLeastOnce`].
    pub fn qos(mut self, qos: Qos) -> Self {
        self.qos = qos;
        self
    }

    /// Publishes messages as retained: the broker keeps the last one per topic and hands it
    /// to new (non-shared) subscribers.
    pub fn retain(mut self, retain: bool) -> Self {
        self.retain = retain;
        self
    }
}

impl MqttPublish {
    pub(crate) fn into_publisher(self, cell: CoreCell) -> MqttPublisher {
        MqttPublisher::new(cell, self.qos, self.retain)
    }
}

impl PublishPolicy<ConnectedMqttBroker> for MqttPublish {
    type Live = MqttPublisher;

    fn pair(
        self,
        connected: &ConnectedMqttBroker,
    ) -> impl Future<Output = Result<Self::Live, PairError>> {
        ready(Ok(connected.publisher_with(self)))
    }
}
