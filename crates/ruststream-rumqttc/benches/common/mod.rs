//! Shared parts of the crate's code-cost benchmarks: what is measured, and how the measurement is
//! kept to one region.
//!
//! # What a scenario looks like
//!
//! One scenario per file, and this module carries what they have in common: the payload, the
//! service setup, the latch a handler counts deliveries down on, and the measurement
//! configuration. The method is the core's, described in its `benches/common` and on the
//! [RustStream benchmarks page](https://powersemmi.github.io/ruststream/latest/benchmarks/).
//!
//! A scenario runs the service a user writes: the app, started through [`RustStream::start`],
//! on [`MqttTestBroker`], this crate's in-process transport. The transport shares the crate's
//! message type, its topic-filter matching and its publisher with the MQTT client path, so what
//! comes out is what a message costs in this crate's code and the framework's above it, with no
//! socket and no server in the number. The comparison against the raw `rumqttc` client, over a
//! real broker, is the other table on the benchmarks page.
//!
//! The in-process transport is behind the `testing` feature, which compiles the framework's test
//! hooks in as well. Outside a `TestApp` run they are installed empty on a single delivery. On a
//! batch the framework copies each payload for the harness's record whether a test runs or not,
//! so the batch scenario counts one allocation per delivery a production service does not make.
//!
//! # Steady state and cold start
//!
//! Every scenario is measured over one delivery, over [`MESSAGES`] deliveries and over twice as
//! many. The slope between the last two is the steady-state cost of a message: everything that
//! happens once is in both totals and cancels in the subtraction. The one-delivery run is the
//! cold start, reported on its own.
//!
//! What a body measures is the start and the drain, in two regions, with the queue filled between
//! them and never counted: producing the messages is not what the scenario is about.
//!
//! # What is counted
//!
//! Collection starts switched off and is switched on for [`measure`], which every body wraps its
//! work in. Everything inside the region is counted: the dispatcher, the codec, this crate's code,
//! the in-process transport, and tokio's share of driving them. [`measure`] is the only frame that
//! carries its name, because a toggle on a name that also appears inside closure types switches
//! collection off again one frame deeper. DHAT is pointed at the same frame; the number read is
//! `Total blocks`, allocations per run.

// Each benchmark target compiles this module on its own and uses the part it needs; what another
// target uses looks unused here.
#![allow(dead_code)]

use std::convert::Infallible;
use std::hint::black_box;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use gungraun::{Callgrind, Dhat, DhatMetric, EntryPoint, EventKind, LibraryBenchmarkConfig};
use ruststream::runtime::{AppInfo, BrokerScope, Identity, RunningApp, RustStream};
use ruststream::{OutgoingMessage, Publisher};
use ruststream_rumqttc::testing::MqttTestBroker;
use serde::Deserialize;
use tokio::runtime::{Builder, Runtime};
use tokio::sync::Notify;

/// The topic every scenario delivers on. A handler names it in its own `#[subscriber(..)]`
/// attribute, which takes a literal.
pub const INPUT: &str = "orders";

/// The values every body carries. Fixed, so that every delivery of a run costs the same.
const ID: u64 = 1_000_000;
const QUANTITY: u32 = 37;

/// The payload every scenario decodes: two integer fields, so a decode allocates nothing and the
/// number is about the crate and the framework rather than about `serde_json`'s string handling.
#[derive(Debug, Deserialize)]
pub struct Order {
    pub id: u64,
    pub quantity: u32,
}

/// Deliveries per measured run: large enough that entering and leaving the region is lost in the
/// per-message number, small enough that a scenario stays under a second of valgrind time.
/// `scripts/bench_results.py` divides by the same count.
pub const MESSAGES: usize = 1_000;

/// The measurement configuration every gated scenario shares.
///
/// `steady` is what one delivery allocates in the steady state and `cold` what starting the
/// service and taking the first delivery allocate once; together they are the hard limit the
/// longest run of the scenario (twice [`MESSAGES`] deliveries) is held to, so the run
/// fails when the path allocates more than it does today. Both are floors the code is held to,
/// so a number that goes down is lowered here in the same change. The instruction limit is
/// relative: `just bench-code --save-baseline=main` records a baseline and
/// `just bench-code --baseline=main` compares against it.
pub fn config(steady: u64, cold: u64) -> LibraryBenchmarkConfig {
    config_every(steady, 1, cold)
}

/// The same for a scenario whose allocations do not come one per delivery: `steady` blocks per
/// `per` deliveries, as a batch handler allocates per batch.
pub fn config_every(steady: u64, per: u64, cold: u64) -> LibraryBenchmarkConfig {
    let mut config = LibraryBenchmarkConfig::default();
    config
        .tool(callgrind().soft_limits([(EventKind::Ir, 2f64)]))
        .tool(dhat().hard_limits([(DhatMetric::TotalBlocks, blocks(steady, per, cold))]));
    config
}

/// The limit for the configured count: the cold part once, plus the steady rate over the longest
/// run of the scenario, which is twice [`MESSAGES`]. The division rounds up.
const fn blocks(steady: u64, per: u64, cold: u64) -> u64 {
    cold + (steady * 2 * MESSAGES as u64).div_ceil(per)
}

/// Callgrind collecting inside the measured region alone.
fn callgrind() -> Callgrind {
    let mut callgrind = Callgrind::with_args([
        "--collect-atstart=no",
        &format!("--toggle-collect={REGION}"),
    ]);
    callgrind.entry_point(EntryPoint::None);
    callgrind
}

/// The measured region: everything this runs is counted, nothing around it is.
#[inline(never)]
pub fn measure<T>(body: impl FnOnce() -> T) -> T {
    body()
}

/// DHAT with a stack window deep enough to reach the measured frame from a publish inside a
/// dispatched handler.
fn dhat() -> Dhat {
    let mut dhat = Dhat::with_args(["--num-callers=128"]);
    dhat.entry_point(EntryPoint::Custom(REGION.to_owned()));
    dhat
}

/// The frame both tools are pointed at.
const REGION: &str = "*common::measure*";

/// A single-threaded runtime: one thread means one order of execution, and the same instruction
/// count on every run.
pub fn runtime() -> Runtime {
    Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("current-thread runtime")
}

/// Counts deliveries down and wakes the benchmark body when the last one has been handled.
///
/// Handlers reach it as the application state. What a delivery pays for it is one relaxed
/// decrement and the branch that reads it.
#[derive(Clone, Debug)]
pub struct Latch(Arc<Inner>);

#[derive(Debug)]
struct Inner {
    remaining: AtomicUsize,
    drained: Notify,
}

impl Default for Latch {
    fn default() -> Self {
        Self(Arc::new(Inner {
            remaining: AtomicUsize::new(0),
            drained: Notify::new(),
        }))
    }
}

impl Latch {
    /// Arms the latch for `count` deliveries.
    pub fn expect(&self, count: usize) {
        self.0.remaining.store(count, Ordering::Release);
    }

    /// Records one handled delivery, waking the waiter on the last one.
    pub fn arrived(&self) {
        if self.0.remaining.fetch_sub(1, Ordering::Relaxed) == 1 {
            self.0.drained.notify_one();
        }
    }

    /// How many deliveries the latch is still waiting for.
    pub fn remaining(&self) -> usize {
        self.0.remaining.load(Ordering::Acquire)
    }

    /// Resolves once every expected delivery has been handled.
    pub async fn drained(&self) {
        while self.0.remaining.load(Ordering::Acquire) > 0 {
            self.0.drained.notified().await;
        }
    }
}

/// The JSON body every delivery carries: the two fields a handler reads.
pub fn json_body() -> Vec<u8> {
    format!("{{\"id\":{ID},\"quantity\":{QUANTITY}}}").into_bytes()
}

/// A service that is built but not started, and what its queue will hold.
///
/// The start is part of the measurement rather than of the setup, because the cold number is
/// what starting costs. It is held as a boxed call so that every scenario hands over the same
/// type; the one indirect call it adds lands in the cold number and nowhere else.
pub struct Pending {
    runtime: Runtime,
    latch: Latch,
    broker: MqttTestBroker,
    start: Box<dyn FnOnce(&Runtime) -> RunningApp>,
    messages: usize,
}

/// The mount a scenario passes in: what `with_broker` does with the scope.
pub type Mount<'a> = &'a mut BrokerScope<MqttTestBroker, Identity, (), Latch>;

/// Builds a one-handler service on a fresh in-process broker, ready to be started by the body.
pub fn pending(messages: usize, mount: impl FnOnce(Mount<'_>)) -> Pending {
    let latch = Latch::default();
    let broker = MqttTestBroker::new();
    let state = latch.clone();
    let app = RustStream::new(AppInfo::new("bench", "0.0.0"))
        .on_startup(async move |()| Ok::<_, Infallible>(state))
        .with_broker(broker.clone(), mount);
    Pending {
        runtime: runtime(),
        latch,
        broker,
        start: Box::new(move |runtime| runtime.block_on(app.start()).expect("the service starts")),
        messages,
    }
}

/// Publishes `count` bodies on [`INPUT`] through the in-process broker's own publisher.
///
/// Part of every setup, never of a measured region: the deliveries are in the queue before the
/// body runs, so what the body pays for is delivery, not production.
fn fill(broker: &MqttTestBroker, runtime: &Runtime, count: usize) {
    let publisher = broker.publisher();
    let body = json_body();
    runtime.block_on(async move {
        for _ in 0..count {
            publisher
                .publish(OutgoingMessage::new(INPUT, &body), None)
                .await
                .expect("in-process publish");
        }
    });
}

/// Starts the service, fills its queue, and drains it: the shape of every scenario here.
///
/// Two measured regions, and the fill between them is in neither. The first is the cold start,
/// the second the deliveries.
pub fn start_and_drain(pending: Pending) {
    let Pending {
        runtime,
        latch,
        broker,
        start,
        messages,
    } = pending;
    let running = measure(|| start(&runtime));
    latch.expect(messages);
    fill(&broker, &runtime, messages);
    assert_eq!(
        latch.remaining(),
        messages,
        "the queue was consumed while it was being filled, so the measured region would be short"
    );
    measure(|| runtime.block_on(latch.drained()));
    black_box(&broker);
    drop(running);
}
