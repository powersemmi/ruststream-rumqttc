//! The integration suite, one test binary: each file under `tests/` is a module of it.

mod live_broker;
mod relay;

mod asyncapi_mqtt;
mod both_modes_mqtt;
mod conformance_mqtt;
mod disconnect_live_mqtt;
mod handlers_live_mqtt;
mod handlers_mqtt;
mod in_process_mqtt;
mod integration_mqtt;
mod overlap_live_mqtt;
mod publish_handover;
mod reconnect_live_mqtt;
mod session_live_mqtt;
