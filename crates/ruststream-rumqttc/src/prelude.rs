//! The imports a service on MQTT writes every time, in one glob.
//!
//! `use ruststream_rumqttc::prelude::*;` brings in the framework's own prelude plus this crate's
//! user-facing surface: the broker, the subscription descriptor and its quality of service, the
//! publish policy, and the per-publish steps. One import is enough for a service file.
//!
//! # Policy names are broker-agnostic
//!
//! Every publish policy this broker supports appears here under its concept name, with the
//! broker prefix stripped: [`MqttPublish`](crate::MqttPublish) is [`Publish`]. A mount site
//! therefore reads the same whichever broker it runs on, and porting a service between brokers
//! changes the import, not the call. The rule cuts both ways: a concept name that is *not* here
//! is one this broker does not have, the same statement the capability manifest below makes
//! about the optional traits.
//!
//! The prefixed names stay at the crate root, so a file that mixes two brokers imports
//! [`MqttPublish`](crate::MqttPublish) explicitly and says which one it means.
//!
//! [`Publish`] is a *policy*: pure declaration, paired with the connected broker to produce a
//! publisher. It is unrelated to the framework's `runtime::Publish`, the builder a publish call
//! returns, which services never name and which this glob does not carry.
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
//! // The policy under its concept name, as a mount site writes it.
//! let policy = Publish::default().qos(Qos::ExactlyOnce).retain(true);
//! # let _ = (broker, topic, policy);
//! ```

// The framework's prelude stops short of brokers on purpose: which broker a service runs on is
// the one thing every service states for itself. Importing this prelude is that statement - the
// broker-specificity lives in the crate path - so the core glob rides along instead of making
// every service file write two imports to say one thing.
pub use ruststream::prelude::*;

// Policies travel under their concept name, stripped of the broker prefix, so a mount site reads
// the same whichever broker it runs on; the prefixed `MqttPublish` stays at the crate root for a
// file that mixes two brokers and has to say which one it means. `Qos` and `MqttPublishOptions`
// are not policies - a descriptor argument and a step trait - so they keep their own names.
pub use crate::{MqttBroker, MqttPublish as Publish, MqttPublishOptions, MqttTopic, Qos};

// This glob is also this broker's capability manifest: it re-exports every optional capability
// trait the connected and live forms implement, so a multi-broker service globbing two preludes
// unifies on the same core items and the compiler checks the overlap. MQTT implements none of
// them - no transactions, no partitions or routing keys, no seekable log, no native
// request/reply, and one PUBLISH packet at a time rather than a batch fetch (see the capability
// table in `docs/mqtt.md` for the protocol reason behind each). Add the trait here when a
// capability is implemented: an empty manifest is a statement about the broker, not an oversight.

// Deliberately absent:
//
// - `testing` (`MqttTestBroker` and its policy): feature-gated broker-author tooling, imported
//   explicitly by the test module that uses it, not by the service the tests cover.
// - The message and connection types (`MqttMessage`, `MqttSubscriber`, `MqttPublisher`,
//   `MqttPublishOverride`, `ConnectedMqttBroker`): a service reaches these through the
//   framework's handler and publish surfaces, which assemble them; code that names one directly
//   is working a layer below a service, and says so by importing it.
// - `MqttError`: a service names errors where it handles them, not everywhere it imports.
