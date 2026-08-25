//! The imports a service on MQTT writes every time, in one glob.
//!
//! Carries the framework's own prelude plus this crate's surface: the broker, the subscription
//! descriptor and its quality of service, the publish policy as [`Publish`], and the per-publish
//! steps.
//!
//! A file that mixes two brokers imports the prefixed [`MqttPublish`](crate::MqttPublish) from
//! the crate root instead.
//!
//! [`Publish`] is the publish policy, not the framework's `runtime::Publish` builder.
//!
//! # Examples
//!
//! ```
//! use ruststream_rumqttc::prelude::*;
//!
//! let broker = MqttBroker::new("mqtt://localhost:1883", "telemetry-svc");
//! let topic = MqttTopic::new("devices/+/telemetry")
//!     .qos(Qos::AtLeastOnce)
//!     .shared("workers");
//! let policy = Publish::default().qos(Qos::ExactlyOnce).retain(true);
//! # let _ = (broker, topic, policy);
//! ```

// Importing this prelude is itself a service's statement of which broker it runs on, which is why
// the core glob rides along here instead of being left to each service file.
pub use ruststream::prelude::*;

// Policies are re-exported under their broker-agnostic concept name; keep the alias when adding
// one.
pub use crate::{MqttBroker, MqttPublish as Publish, MqttPublishOptions, MqttTopic, Qos};

// Capability manifest deliberately empty: MQTT implements none of the seven (see the capability
// table in docs/mqtt.md); add a trait here when a capability lands.

// Deliberately absent, do not add:
// - `testing`: feature-gated broker-author tooling, imported by the tests that use it.
// - `MqttMessage`, `MqttSubscriber`, `MqttPublisher`, `MqttPublishOverride`,
//   `ConnectedMqttBroker`: a service reaches these through the framework's own surfaces.
// - `MqttError`: named where it is handled.
