//! What the in-process stand does with the buffer the framework hands it on the publish path.
//!
//! The stand declares `Take`, the way the real publisher does: a recorded delivery keeps its
//! payload, as a queued PUBLISH packet keeps it. Content equality cannot tell a hand-over from a
//! copy, so the payload is followed by the address it was written at, and the header map by what a
//! publish carrying it allocates.

#![cfg(feature = "testing")]

use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;

use bytes::Bytes;
use ruststream::testing::TestableBroker;
use ruststream::{Broker, BytesMut, HeaderMap, OutgoingMessage, Publisher, Str};
use ruststream_rumqttc::testing::MqttTestBroker;

/// Counts this thread's allocations, so the cost of one publish can be read off directly. A
/// thread-local count rather than a global one: the other tests in this binary are none of this
/// measurement's business.
struct Counting;

thread_local! {
    static ALLOCATIONS: Cell<usize> = const { Cell::new(0) };
}

unsafe impl GlobalAlloc for Counting {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        ALLOCATIONS.with(|count| count.set(count.get() + 1));
        // SAFETY: the layout is the caller's, forwarded unchanged to the system allocator.
        unsafe { System.alloc(layout) }
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        // SAFETY: the pointer and layout are the caller's, forwarded unchanged.
        unsafe { System.dealloc(ptr, layout) }
    }
}

#[global_allocator]
static ALLOCATOR: Counting = Counting;

/// What this thread has allocated so far.
fn allocations() -> usize {
    ALLOCATIONS.with(Cell::get)
}

/// Two headers over buffers that are already shared, so copying the map costs its table and
/// nothing per entry - one allocation, whatever the machine.
fn headers() -> HeaderMap {
    let mut headers = HeaderMap::new();
    headers.insert(
        Str::from_static("content-type"),
        Bytes::from_static(b"application/json"),
    );
    headers.insert(Str::from_static("x-tenant"), Bytes::from_static(b"acme"));
    headers
}

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

/// Publishes one message through a stand of its own and answers what that publish allocated.
///
/// The stand is fresh and the message is built before the count starts, so the figure is the
/// conversion's alone; a warm-up publish first grows the log this destination keeps.
async fn cost_of_publishing(carried: HeaderMap) -> usize {
    let connected = MqttTestBroker::new()
        .connect()
        .await
        .expect("the in-process broker connects");
    let publisher = connected.publisher();
    publisher
        .publish(
            OutgoingMessage::produced("orders", BytesMut::from(&b"{}"[..])),
            None,
        )
        .await
        .expect("the stand accepts the publish");

    let message =
        OutgoingMessage::produced("orders", BytesMut::from(&b"{}"[..])).with_headers(carried);
    let before = allocations();
    publisher
        .publish(message, None)
        .await
        .expect("the stand accepts the publish");
    allocations() - before
}

/// The map the publish filled travels into the delivery instead of being copied into it.
///
/// The stand keeps one copy of its own, for the published log a test asserts against. Anything
/// beyond that is the conversion copying a map it was handed to keep.
#[tokio::test]
async fn the_header_map_travels_into_the_delivery() {
    let map = headers();
    let one_copy = {
        let before = allocations();
        let copy = map.clone();
        let cost = allocations() - before;
        drop(copy);
        cost
    };
    assert_eq!(
        one_copy, 1,
        "a map over shared buffers copies its table and nothing else"
    );

    let bare = cost_of_publishing(HeaderMap::new()).await;
    let carried = cost_of_publishing(map).await;

    assert_eq!(
        carried - bare,
        one_copy,
        "the stand records one copy of the map; the conversion must not make a second",
    );
}
