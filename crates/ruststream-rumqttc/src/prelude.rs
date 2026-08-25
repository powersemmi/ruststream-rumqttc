//! The imports a service on MQTT writes every time, in one glob.
//!
//! `use ruststream_rumqttc::prelude::*;` brings in the framework's own prelude plus this crate's
//! user-facing surface: the broker, the subscription descriptor and its quality of service, the
//! publish policy, and the per-publish steps. One import is enough for a service file.
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
//! # let _ = (broker, topic);
//! ```

// The framework's prelude stops short of brokers on purpose: which broker a service runs on is
// the one thing every service states for itself. Importing this prelude is that statement - the
// broker-specificity lives in the crate path - so the core glob rides along instead of making
// every service file write two imports to say one thing.
pub use ruststream::prelude::*;

// The publish policy and the per-publish steps. `MqttPublishOptions` is an extension trait, and a
// glob is exactly how a user reaches `with_qos` / `with_retain` without naming it.
pub use crate::{MqttBroker, MqttPublish, MqttPublishOptions, MqttTopic, Qos};

// Deliberately absent:
//
// - `testing` (`MqttTestBroker` and its policy): feature-gated broker-author tooling, imported
//   explicitly by the test module that uses it, not by the service the tests cover.
// - The message and connection types (`MqttMessage`, `MqttSubscriber`, `MqttPublisher`,
//   `MqttPublishOverride`, `ConnectedMqttBroker`): a service reaches these through the
//   framework's handler and publish surfaces, which assemble them; code that names one directly
//   is working a layer below a service, and says so by importing it.
// - `MqttError`: a service names errors where it handles them, not everywhere it imports.
