//! MQTT 5 broker implementation for `RustStream`, built on `rumqttc`.
//!
//! Handlers, routers, codecs, and middleware come from the framework; this crate supplies
//! the transport over [`rumqttc`](https://docs.rs/rumqttc), targeting MQTT 5 because two
//! things the framework relies on exist only there: user properties (headers travel natively
//! instead of inside a wrapper envelope) and shared subscriptions (which make competing
//! consumers expressible).
//!
//! - The crate owns a connection task that drives the client's single event loop,
//!   demultiplexes packets to per-subscription streams by topic-filter matching, reconnects
//!   with backoff, and resubscribes when the broker reports the session gone, without
//!   stalling keep-alive traffic.
//! - Acknowledgement follows the quality of service: `QoS` 1/2 acknowledge through the
//!   protocol under manual control; `QoS` 0 has no protocol acknowledgement and reports
//!   [`AckError::Unsupported`](ruststream::AckError::Unsupported), as does
//!   `nack(requeue = true)` - MQTT has no negative acknowledgement, and unacked
//!   messages redeliver when a persistent session resumes.
//! - Retained messages, last will, session persistence, and TLS with client certificates are
//!   configuration on the broker and the publish policy. The two arguments MQTT carries on every
//!   PUBLISH packet, quality of service and the retain flag, are also available per message
//!   through [`MqttPublishOptions`], which feeds them into the framework's publish builder.

#![forbid(unsafe_code)]

mod broker;
mod conn;
mod error;
mod filter;
mod message;
pub mod prelude;
mod publisher;
mod subscriber;
#[cfg(feature = "testing")]
pub mod testing;

pub use broker::{ConnectedMqttBroker, MqttBroker};
pub use error::MqttError;
pub use filter::{MqttTopic, Qos};
pub use message::MqttMessage;
pub use publisher::{MqttPublish, MqttPublishOptions, MqttPublishOverride, MqttPublisher};
pub use subscriber::MqttSubscriber;
