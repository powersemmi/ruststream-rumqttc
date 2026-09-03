//! The imports a service on MQTT writes every time, in one glob.
//!
//! Carries the framework's own prelude plus this crate's surface: the broker, the subscription
//! descriptor and its quality of service, the [`MqttPublish`] policy, and the per-publish steps.
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
//! let policy = MqttPublish::default().qos(Qos::ExactlyOnce).retain(true);
//! # let _ = (broker, topic, policy);
//! ```

// Importing this prelude is itself a service's statement of which broker it runs on, which is why
// the core glob rides along here instead of being left to each service file.
pub use ruststream::prelude::*;

// Every name here stays prefixed. An unprefixed alias would win over the glob above rather than
// clash with it, so a framework name this crate happened to reuse would go silently missing from
// a service that imports the prelude: `Publish`, the framework's slot capability trait, is the
// live case.
pub use crate::{MqttBroker, MqttPublish, MqttPublishOptions, MqttTopic, Qos};

// Capability manifest deliberately empty: MQTT implements none of the seven (see the capability
// table in docs/mqtt.md); add a trait here when a capability lands.

// Deliberately absent, do not add:
// - `testing`: feature-gated broker-author tooling, imported by the tests that use it.
// - `MqttMessage`, `MqttSubscriber`, `MqttPublisher`, `MqttPublishOverride`,
//   `ConnectedMqttBroker`: a service reaches these through the framework's own surfaces.
// - `MqttError`: named where it is handled.

#[cfg(test)]
mod tests {
    use super::*;

    // A shadowed framework name reports nothing here and nothing at the re-export: the error
    // lands in the service that imported this prelude, naming a type where it wanted a trait.
    // These bounds move it back to this crate, where the shadowing would be introduced.
    fn _publish_is_the_framework_slot_trait<T: Publish>(_: T) {}
    fn _publisher_is_the_framework_trait<T: Publisher>(_: T) {}

    fn _the_policy_keeps_its_prefixed_name() -> MqttPublish {
        MqttPublish::default()
    }
}
