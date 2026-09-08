//! In-process test support, behind the `testing` feature.
//!
//! [`MqttTestBroker`] is a handler-stub transport that reproduces the crate's core routing in
//! memory - no server, no network - and implements
//! [`TestableBroker`](ruststream::testing::TestableBroker) on its connected form, so
//! application handlers can be unit-tested with the
//! [`TestApp`](ruststream::testing::TestApp) harness. It routes by topic-filter match, the rule
//! the connection task demultiplexes deliveries with, so a subscription declared with
//! [`MqttTopic`](crate::MqttTopic) mounts here as it is written for the real broker, wildcards
//! included. [`MqttPublish`](crate::MqttPublish) pairs against it in the same way, so a routes
//! file is mounted here as written, both halves of it - there is no in-process descriptor and no
//! in-process policy to swap in.
//!
//! What the descriptor asks for is honoured as far as the answer is observable without a server: a
//! share group makes its members compete for one delivery instead of each taking a copy, and the
//! quality of service - the lesser of the publish's and the subscription's, as on the wire -
//! decides whether a delivery can be settled at all, so a `QoS` 0 delivery reports
//! [`AckError::Unsupported`](ruststream::AckError::Unsupported) here exactly as it does live.
//!
//! What is left out is the protocol itself: the acknowledgement exchange behind an acknowledged
//! `QoS`, retained messages, the session that redelivers, dead-letter timing. A test here proves
//! what a handler saw, how it settled, and what it published; it cannot prove that a guarantee was
//! kept on a wire. That is what the live suite is for, and each unmodelled behaviour says at its
//! own definition what a test must not read into it.
//!
//! The framework's own contract suites run against this transport, not only against a server:
//! `conformance::harness::run_suite` for routing, `conformance::harness::lifecycle` for the
//! ladder, and `conformance::capabilities::batches` for the one capability this broker implements.
//! Whatever the core means by correct broker behaviour, the stand-in is held to it.

mod broker;
mod router;
mod subscriber;

pub use broker::{ConnectedMqttTestBroker, MqttTestBroker, MqttTestPublisher};
pub use subscriber::{MqttTestMessage, MqttTestSubscriber};
