//! MQTT 5 broker implementation for `RustStream`, built on `rumqttc`.
//!
//! Handlers, routers, codecs, and middleware come from the framework; this crate supplies
//! the transport over [`rumqttc`](https://docs.rs/rumqttc), targeting MQTT 5 because two
//! things the framework relies on exist only there: user properties (headers travel natively
//! instead of through an invented envelope) and shared subscriptions (competing consumers
//! are expressible at all).
//!
//! - The crate owns a connection task that drives the client's single event loop,
//!   demultiplexes packets to per-subscription streams by topic-filter matching, reconnects
//!   with backoff, and resubscribes when the broker reports the session gone - all without
//!   ever stalling keep-alive traffic.
//! - Acknowledgement follows the quality of service: `QoS` 1/2 acknowledge through the
//!   protocol under manual control; `QoS` 0 reports
//!   [`AckError::Unsupported`](ruststream::AckError::Unsupported) rather than pretending, as
//!   does `nack(requeue = true)` - MQTT has no negative acknowledgement, and unacked
//!   messages redeliver when a persistent session resumes.
//! - Retained messages, last will, session persistence, and TLS with client certificates are
//!   configuration on the broker and the publish policy.

#![forbid(unsafe_code)]

mod broker;
mod conn;
mod error;
mod filter;
mod message;
mod publisher;
mod subscriber;
#[cfg(feature = "testing")]
pub mod testing;

pub use broker::{ConnectedMqttBroker, MqttBroker};
pub use error::MqttError;
pub use filter::{MqttTopic, Qos};
pub use message::MqttMessage;
pub use publisher::{MqttPublish, MqttPublisher};
pub use subscriber::MqttSubscriber;
