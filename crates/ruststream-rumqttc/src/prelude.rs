//! The imports a routes file on MQTT writes every time, in one glob.
//!
//! Carries the framework's own prelude plus this crate's surface: the broker, the subscription
//! descriptor and its quality of service, the publish policy as [`Publish`], and the per-publish
//! steps.
//!
//! A handler file needs none of it: a body binds its injected publisher with a capability trait
//! and names no broker type, so it imports the framework's prelude alone. This glob is the mount
//! site's, and importing it is the statement of which broker the routes run on.
//!
//! A file that mixes two brokers imports the prefixed [`MqttPublish`](crate::MqttPublish) from
//! the crate root instead.
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
// one. The name is this glob's to give: a handler bounds its slot with a capability trait through
// the framework's prelude, and never sees this one.
pub use crate::{MqttBroker, MqttPublish as Publish, MqttPublishOptions, MqttTopic, Qos};

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

    // The two vocabularies this glob has to serve, pinned where they are handed out rather than
    // in the service that would otherwise meet the failure: the capability a handler binds with
    // stays a trait, and the policy a mount site attaches answers to its concept name.
    fn _the_capability_a_handler_binds_with_is_a_trait<T: Publisher>() {}

    #[test]
    fn the_policy_answers_to_its_concept_name() {
        let _: Publish = Publish::default();
    }
}
