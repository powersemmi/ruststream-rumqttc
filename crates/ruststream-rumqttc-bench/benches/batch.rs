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
//! Consuming in batches of 64 at `QoS` 1: the crate assembles the batch on the client, hands the
//! handler a slice, and the runtime acknowledges every delivery in it.

mod common;

use std::hint::black_box;

use common::{Latch, MESSAGES, Order, Pending};
use gungraun::{library_benchmark, library_benchmark_group, main};
use ruststream_rumqttc::prelude::*;

#[subscriber(MqttTopic::new("bench/orders").qos(Qos::AtLeastOnce))]
async fn consume(orders: &[Order], ctx: &mut Context<'_, (), Latch>) -> HandlerOutcome {
    for order in orders {
        black_box((order.id, order.quantity));
        ctx.state().arrived();
    }
    HandlerOutcome::ack()
}

fn app(messages: usize) -> Pending {
    common::pending(messages, |b| {
        b.include(consume.batch(nonzero!(64)));
    })
}

// The allocations are the client's as much as the crate's, and a few of them move with how the
// socket's reads split, so the floor is the highest count of three runs plus one percent, stated
// over a thousand deliveries.
#[library_benchmark]
#[bench::first(args = (app(1)), config = common::config_every(4_206, 1_000, 70, 1))]
#[bench::base(args = (app(MESSAGES)), config = common::config_every(4_206, 1_000, 70, MESSAGES))]
#[bench::twice(args = (app(2 * MESSAGES)), config = common::config_every(4_206, 1_000, 70, 2 * MESSAGES))]
fn service(app: Pending) {
    common::start_and_drain(app);
}

library_benchmark_group!(name = batch_group; benchmarks = service);
main!(library_benchmark_groups = batch_group);
