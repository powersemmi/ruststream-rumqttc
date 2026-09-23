// The harness macros generate the group module, its items and the paths between them, and a
// benchmark function takes its setup value by value because the harness owns the drop; the
// crate's lints are written for the library surface, not for generated benchmark scaffolding.
#![allow(
    missing_docs,
    unused_qualifications,
    unreachable_pub,
    clippy::must_use_candidate,
    clippy::needless_pass_by_value
)]
//! Consuming a small JSON body at `QoS` 1: the subscription yields the crate's message, the
//! dispatcher decodes it into a struct, the handler reads a field, and the runtime acknowledges it
//! with a `PUBACK`.

mod common;

use std::hint::black_box;

use common::{Latch, MESSAGES, Order, Pending};
use gungraun::{library_benchmark, library_benchmark_group, main};
use ruststream_rumqttc::prelude::*;

#[subscriber(MqttTopic::new("bench/orders").qos(Qos::AtLeastOnce))]
async fn consume(order: &Order, ctx: &mut Context<'_, (), Latch>) -> HandlerOutcome {
    black_box((order.id, order.quantity));
    ctx.state().arrived();
    HandlerOutcome::ack()
}

fn app(messages: usize) -> Pending {
    common::pending(messages, |b| {
        b.include(consume);
    })
}

// The allocations are the client's as much as the crate's, and a few of them move with how the
// socket's reads split, so the floor is the highest count of three runs plus one percent, stated
// over a thousand deliveries.
#[library_benchmark(config = common::config_every(4_189, 1_000, 66))]
#[bench::first(app(1))]
#[bench::base(app(MESSAGES))]
#[bench::twice(app(2 * MESSAGES))]
fn service(app: Pending) {
    common::start_and_drain(app);
}

library_benchmark_group!(name = consume_group; benchmarks = service);
main!(library_benchmark_groups = consume_group);
