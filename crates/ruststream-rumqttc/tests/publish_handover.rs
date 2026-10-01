//! What the in-process mode does with the buffer the framework hands it on the publish path.
//!
//! The publisher declares `Take`: a queued PUBLISH packet keeps its payload, and the in-process
//! transport keeps it the same way. Content equality cannot tell a hand-over from a copy, so the
//! payload is followed by the address it was written at.

#![cfg(feature = "testing")]

use ruststream::testing::{InProcess, TestableBroker};
use ruststream::{BytesMut, OutgoingMessage, Publisher};
use ruststream_rumqttc::MqttBroker;

/// The message the server logs is the buffer the publish wrote, not a copy of it.
#[tokio::test]
async fn the_server_keeps_the_buffer_it_was_handed() {
    let connected = MqttBroker::new("mqtt://localhost:1883", "handover")
        .connect_in_process()
        .await
        .expect("the production broker connects in process");
    let publisher = connected.publisher();

    let body = BytesMut::from(&br#"{"id":7}"#[..]);
    let written_at = body.as_ptr();
    publisher
        .publish(OutgoingMessage::produced("orders", body), None)
        .await
        .expect("the server accepts the publish");

    assert_eq!(
        connected
            .published("orders")
            .first()
            .expect("the publish is logged")
            .payload()
            .as_ptr(),
        written_at,
        "the logged message must carry the buffer the publish wrote, not a copy of it",
    );
}
