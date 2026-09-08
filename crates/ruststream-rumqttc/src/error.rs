//! The crate-level error type.

use std::error::Error as StdError;

/// Errors returned by the MQTT 5 broker.
///
/// One enum for the whole crate, variants by source, per the `RustStream` broker conventions.
/// The wrapped sources are boxed `std` errors so the public API does not leak the client's
/// error types.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum MqttError {
    /// Establishing the connection failed (transport, TLS, or the broker refused the
    /// connection with a non-retryable reason).
    #[error("mqtt connection error: {0}")]
    Connect(#[source] Box<dyn StdError + Send + Sync>),

    /// Subscribing failed (the broker rejected the filter, or the connection died).
    #[error("mqtt subscribe error on '{filter}': {reason}")]
    Subscribe {
        /// The topic filter the subscription targeted.
        filter: String,
        /// The rejection or transport reason.
        reason: String,
    },

    /// The connection failed permanently while receiving.
    #[error("mqtt receive error: {0}")]
    Receive(String),

    /// Publishing failed (the connection task is gone, or the topic is invalid).
    #[error("mqtt publish error to '{topic}': {reason}")]
    Publish {
        /// The topic the message targeted.
        topic: String,
        /// The failure reason.
        reason: String,
    },

    /// The handle is used before `connect` filled the shared connection, or after `shutdown`.
    #[error("mqtt broker is not connected")]
    NotConnected,

    /// A per-message publish argument arrived on its header with a value this crate cannot read
    /// ([`QOS_HEADER`](crate::QOS_HEADER), [`RETAIN_HEADER`](crate::RETAIN_HEADER)).
    ///
    /// The publish is refused rather than sent under the publisher's policy: the call asked for
    /// a delivery guarantee, and sending under another one silently is the failure this reports.
    #[error("invalid mqtt publish argument: the {header} header carries {expected}; got {value:?}")]
    InvalidPublishArgument {
        /// The header the value arrived on.
        header: &'static str,
        /// The value as it arrived, lossily decoded for the message.
        value: String,
        /// The vocabulary that header accepts.
        expected: &'static str,
    },

    /// A broker option or subscription descriptor is invalid.
    #[error("invalid mqtt descriptor: {0}")]
    Invalid(String),
}
