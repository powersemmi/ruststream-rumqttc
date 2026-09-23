//! Shared parts of the crate's code-cost benchmarks: what is measured, and how the measurement is
//! kept to one region.
//!
//! # What a scenario looks like
//!
//! One scenario per file, and this module carries what they have in common: the payload, the
//! service setup, the feeder, the latch a handler counts deliveries down on, and the measurement
//! configuration. The method is the core's, described in its `benches/common` and on the
//! [RustStream benchmarks page](https://powersemmi.github.io/ruststream/latest/benchmarks/).
//!
//! A scenario runs the service a user writes on the real broker: the app, started through
//! [`RustStream::start`] on [`MqttBroker`], connected to the mosquitto of the compose stand
//! (`MQTT_TEST_URL`, `mqtt://127.0.0.1:1883` when unset). What comes out is what a message costs
//! on the service's thread: the framework, this crate, and the `rumqttc` client's work on that
//! thread, since the crate drives the client's event loop from a task of the service's runtime.
//!
//! # Steady state and cold start
//!
//! Every scenario is measured over one delivery, over [`MESSAGES`] deliveries and over twice as
//! many. The slope between the last two is the steady-state cost of a message: everything that
//! happens once is in both totals and cancels in the subtraction. The one-delivery run is the
//! cold start - the connect, the subscription and the first delivery - reported on its own.
//!
//! # The fill
//!
//! Between the two regions a feeder on a thread and a runtime of its own publishes the run's
//! messages at `QoS` 1 and waits for every acknowledgement of the broker. The service's runtime has
//! one thread and runs only inside `block_on`, so nothing is consumed while the feeder publishes:
//! the broker holds the messages for the live session, up to the client's receive maximum in
//! flight and the rest queued, and the drain region takes them all. The feeder's work is on its
//! own thread and is never counted.
//!
//! # What is counted
//!
//! Collection starts switched off and is switched on for [`measure`], which every body wraps its
//! work in. Everything the service's thread runs inside the region is counted; waiting on the
//! socket is not, since valgrind counts instructions and not time. [`measure`] is the only frame
//! that carries its name, because a toggle on a name that also appears inside closure types
//! switches collection off again one frame deeper. DHAT is pointed at the same frame; the number
//! read is `Total blocks`, allocations per run.

// Each benchmark target compiles this module on its own and uses the part it needs; what another
// target uses looks unused here.
#![allow(dead_code)]

use std::convert::Infallible;
use std::env;
use std::hint::black_box;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::thread;

use gungraun::{Callgrind, Dhat, DhatMetric, EntryPoint, EventKind, LibraryBenchmarkConfig};
use rumqttc::v5::mqttbytes::QoS;
use rumqttc::v5::mqttbytes::v5::Packet;
use rumqttc::v5::{AsyncClient, Event, MqttOptions};
use ruststream::runtime::{AppInfo, BrokerScope, Identity, RunningApp, RustStream};
use ruststream_rumqttc::MqttBroker;
use serde::Deserialize;
use tokio::runtime::{Builder, Runtime};
use tokio::sync::Notify;

/// The topic every scenario delivers on. A handler names it in its own `#[subscriber(..)]`
/// attribute, which takes the literal.
pub const INPUT: &str = "bench/orders";

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

/// Deliveries per measured run.
///
/// The longest run, twice this, fits the client's receive maximum of 1000 in-flight messages, so
/// the broker holds the whole fill for the session without queueing past its own limit.
/// `scripts/bench_results.py` divides by the same count.
pub const MESSAGES: usize = 500;

/// The measurement configuration every scenario shares.
///
/// `steady` is what one delivery allocates in the steady state and `cold` what starting the
/// service and taking the first delivery allocate once; together they are the hard limit the
/// longest run of the scenario (twice [`MESSAGES`] deliveries) is held to, so the run fails when
/// the path allocates more than it does today. Both are floors the code is held to, so a number
/// that goes down is lowered here in the same change. The instruction limit is relative:
/// `just bench-code --save-baseline=main` records a baseline and `just bench-code
/// --baseline=main` compares against it.
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

/// The limit: the cold part once, plus the steady rate over the longest run of the scenario,
/// which is twice [`MESSAGES`]. The division rounds up.
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
    // `black_box` runs after the body returns, so the call cannot become a tail jump: DHAT
    // attributes an allocation to this region only while this frame is on the stack.
    black_box(body())
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

/// A single-threaded runtime: the service's work on one thread, in one order.
pub fn runtime() -> Runtime {
    Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("current-thread runtime")
}

/// The broker the stand exposes.
fn url() -> String {
    env::var("MQTT_TEST_URL").unwrap_or_else(|_| "mqtt://127.0.0.1:1883".to_owned())
}

/// Client identifiers of this process, so a run never shares a session with another.
fn client_id(role: &str) -> String {
    format!("ruststream-bench-{role}-{}", std::process::id())
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

/// A service that is built but not started, and how many messages its run takes.
///
/// The start is part of the measurement rather than of the setup, because the cold number is
/// what starting costs. It is held as a boxed call so that every scenario hands over the same
/// type; the one indirect call it adds lands in the cold number and nowhere else.
pub struct Pending {
    runtime: Runtime,
    latch: Latch,
    start: Box<dyn FnOnce(&Runtime) -> RunningApp>,
    messages: usize,
}

/// The mount a scenario passes in: what `with_broker` does with the scope.
pub type Mount<'a> = &'a mut BrokerScope<MqttBroker, Identity, (), Latch>;

/// Builds a one-handler service on the stand's broker, ready to be started by the body.
pub fn pending(messages: usize, mount: impl FnOnce(Mount<'_>)) -> Pending {
    let latch = Latch::default();
    let state = latch.clone();
    let app = RustStream::new(AppInfo::new("bench", "0.0.0"))
        .on_startup(async move |()| Ok::<_, Infallible>(state))
        .with_broker(MqttBroker::new(url(), client_id("service")), mount);
    Pending {
        runtime: runtime(),
        latch,
        start: Box::new(move |runtime| runtime.block_on(app.start()).expect("the service starts")),
        messages,
    }
}

/// Publishes `count` bodies on [`INPUT`] at `QoS` 1 from a thread and a runtime of its own, and
/// returns once the broker acknowledged every one of them.
///
/// The raw client rather than this crate's publisher: the feeder is setup, and what it must
/// guarantee is that the broker holds every message before the drain starts, which a `PUBACK`
/// count says directly.
fn fill(count: usize) {
    thread::spawn(move || {
        runtime().block_on(async move {
            let (host, port) = endpoint(&url());
            let mut options = MqttOptions::new(client_id("feeder"), host, port);
            options.set_max_packet_size(Some(1 << 20));
            let (client, mut events) = AsyncClient::new(options, 64);
            let body = json_body();
            let publishing = tokio::spawn(async move {
                for _ in 0..count {
                    client
                        .publish(INPUT, QoS::AtLeastOnce, false, body.clone())
                        .await
                        .expect("the feeder publishes");
                }
                client
            });
            let mut acknowledged = 0;
            while acknowledged < count {
                if let Event::Incoming(Packet::PubAck(_)) =
                    events.poll().await.expect("the feeder's connection holds")
                {
                    acknowledged += 1;
                }
            }
            let client = publishing.await.expect("the feeder's task ends");
            client.disconnect().await.expect("the feeder disconnects");
        });
    })
    .join()
    .expect("the feeder thread ends");
}

/// The host and port of an `mqtt://` URL.
fn endpoint(url: &str) -> (String, u16) {
    let rest = url.strip_prefix("mqtt://").unwrap_or(url);
    let (host, port) = rest
        .rsplit_once(':')
        .expect("the broker URL names a host and a port");
    (
        host.to_owned(),
        port.parse().expect("the broker URL's port is a number"),
    )
}

/// Starts the service, fills its subscription, and drains it: the shape of every scenario here.
///
/// Two measured regions, and the fill between them is in neither. The first is the cold start,
/// the second the deliveries.
pub fn start_and_drain(pending: Pending) {
    let Pending {
        runtime,
        latch,
        start,
        messages,
    } = pending;
    let running = measure(|| start(&runtime));
    latch.expect(messages);
    fill(messages);
    assert_eq!(
        latch.remaining(),
        messages,
        "the subscription was consumed while it was being filled, so the measured region would be \
         short"
    );
    measure(|| runtime.block_on(latch.drained()));
    runtime
        .block_on(running.shutdown())
        .expect("the service stops");
}
