//! In-process test support, behind the `testing` feature.
//!
//! [`MqttTestBroker`] is a handler-stub transport that reproduces the crate's core routing in
//! memory - no server, no network - and implements
//! [`TestableBroker`](ruststream::testing::TestableBroker) on its connected form, so
//! application handlers can be unit-tested with the
//! [`TestApp`](ruststream::testing::TestApp) harness. It routes by topic-filter match, the rule
//! the connection task demultiplexes deliveries with, so a subscription declared with
//! [`MqttTopic`](crate::MqttTopic) mounts here as it is written for the real broker, wildcards
//! included.
//!
//! What a descriptor asks for past address selection is protocol behaviour, and this transport
//! has no protocol: the quality of service, the retain flag, and the broker-side distribution of
//! a shared group are ignored, as are session redelivery and dead-letter timing. A test here
//! therefore proves that a handler sees what a broker would route to it; it cannot prove a
//! delivery guarantee. Those are verified end to end against a real broker, and each ignored
//! option says at its own definition what a test must not read into it.

mod broker;
mod router;
mod subscriber;

pub use broker::{ConnectedMqttTestBroker, MqttTestBroker, MqttTestPublish, MqttTestPublisher};
pub use subscriber::{MqttTestMessage, MqttTestSubscriber};
