//! What the in-process stand does with the buffer the framework hands it on the publish path.
//!
//! The stand declares `Take`, the way the real publisher does: a recorded delivery keeps its
//! payload, as a queued PUBLISH packet keeps it. Content equality cannot tell a hand-over from a
//! copy, so the payload is followed by the address it was written at.

#![cfg(feature = "testing")]

use ruststream::testing::TestableBroker;
use ruststream::{Broker, BytesMut, OutgoingMessage, Publisher};
use ruststream_rumqttc::testing::MqttTestBroker;

/// The delivery the stand records is the buffer the publish wrote, not a copy of it.
#[tokio::test]
async fn the_stand_records_the_buffer_it_was_handed() {
    let connected = MqttTestBroker::new()
        .connect()
        .await
        .expect("the in-process broker connects");
    let publisher = connected.publisher();

    let body = BytesMut::from(&br#"{"id":7}"#[..]);
    let written_at = body.as_ptr();
    publisher
        .publish(OutgoingMessage::produced("orders", body), None)
        .await
        .expect("the stand accepts the publish");

    assert_eq!(
        connected
            .published("orders")
            .first()
            .expect("the publish is logged")
            .payload()
            .as_ptr(),
        written_at,
        "the delivery must carry the buffer the publish wrote, not a copy of it",
    );
}
