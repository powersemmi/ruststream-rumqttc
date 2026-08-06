//! In-process test support, behind the `testing` feature.
//!
//! [`MqttTestBroker`] is a handler-stub transport that reproduces the crate's core routing in
//! memory - no server, no network - and implements
//! [`TestableBroker`](ruststream::testing::TestableBroker) on its connected form, so
//! application handlers can be unit-tested with the
//! [`TestApp`](ruststream::testing::TestApp) harness. It routes by exact address match and does
//! not simulate broker-specific semantics (dead-letter policies, credit, redelivery timing);
//! those are verified end to end against a real broker.

mod broker;
mod router;
mod subscriber;

pub use broker::{ConnectedMqttTestBroker, MqttTestBroker, MqttTestPublish, MqttTestPublisher};
pub use subscriber::{MqttTestMessage, MqttTestSubscriber};
