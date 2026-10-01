// The benchmark is a binary of its own, not library surface: the framework's macros generate the
// handler scaffolding, and a measured loop panics on a broker fault rather than threading a
// `Result` through a scenario nobody recovers from.
#![allow(
    missing_docs,
    unreachable_pub,
    unused_qualifications,
    clippy::cast_precision_loss,
    clippy::cast_possible_truncation,
    clippy::cast_sign_loss
)]
//! What this crate, and then the framework on top of it, cost over the `rumqttc` client.
//!
//! Every scenario runs three times over, as three loops that differ in one thing each: what
//! carries the messages.
//!
//! - **raw** drives `rumqttc` directly.
//! - **adapter** drives this crate's own types - the broker, the subscription descriptor, the
//!   [`Subscriber`] stream it yields, the [`IncomingMessage`] and its `ack`, the [`Publisher`] -
//!   from a loop written here. No handler, no app, no dispatch.
//! - **framework** is the service a user writes: a `#[subscriber]` handler mounted on an app the
//!   runtime starts.
//!
//! `adapter` against `raw` is what this crate's consumer and publisher cost over the client they
//! wrap, which is the question this repository answers. `framework` against `adapter` is what the
//! runtime costs on top of this broker in particular, which is a finding about how the two meet
//! rather than about the runtime alone.
//!
//! Everything else is identical across the three: same connection options, same quality of
//! service on the subscription and on the publish, same position for the acknowledgement, same
//! decode into the same type, the same payload bytes, the same tokio runtime and the same binary.
//! The procedure the numbers follow is the framework's own, published at
//! <https://powersemmi.github.io/ruststream/latest/benchmarks/>.
//!
//! # The shape of the raw loop
//!
//! `rumqttc` gives the caller an event loop to drive, and what a delivery costs depends on where
//! that loop runs. This crate polls it on a task of its own and hands deliveries to the
//! subscription over a channel, so the raw loop does the same, with the same client channel
//! capacity. A loop polled from inside the consuming task would be a different concurrency shape
//! rather than a different consumer, and the number would be about that.
//!
//! Every loop feeds itself on a second connection of its own. The adapter and the framework feed
//! through this crate's publisher, because a service publishes through exactly that: what
//! separates those two is the dispatch on the consuming side and nothing else.
//!
//! # What a run is
//!
//! The subscription is opened first, the feeding connection then publishes into it, and the
//! window runs from the first delivery to the end of the last one. Connecting and subscribing are
//! startup cost and sit outside it. Every run gets a fresh topic and fresh client identifiers, so
//! a run never sees what the one before it left behind.
//!
//! The message count is not a constant: a probe run measures the raw loop's rate and the count is
//! set from it, so a measured run lasts at least [`SECONDS`] on whatever machine it is taken on.
//!
//! Rounds are interleaved - raw, adapter, framework, raw, adapter, framework - and each loop
//! reports its best, median and worst round. The best is the headline: noise only ever slows a
//! run down, so the fastest round is the closest to the undisturbed cost. The distance between
//! the best and the worst is the noise a difference has to clear. Running one loop to the end and
//! then the next would charge every drift of the machine to whichever ran last.
//!
//! # What the numbers do not say
//!
//! The window ends where the last delivery has been decoded and read rather than after its
//! acknowledgement, on all three loops alike: the framework acknowledges a delivery once the
//! handler is done, which is a point the handler itself cannot observe. One acknowledgement out
//! of hundreds of thousands is far below the run-to-run spread.
//!
//! Whether the transport paced a run is measured rather than guessed, and on MQTT the quality of
//! service decides it. A probe outside every loop times one round trip against the same broker,
//! and a row is marked broker-bound when the round trips a delivery costs the consumer are worth
//! at least half of the time a message took. At `QoS` 1 that is one round trip, the
//! acknowledgement travelling back; at `QoS` 0 it is none, because nothing travels back at all.

use std::convert::Infallible;
use std::env;
use std::fmt::Write as _;
use std::hint::black_box;
use std::iter::repeat_n;
use std::num::NonZeroUsize;
use std::pin::pin;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use bytes::Bytes;
use futures::StreamExt as _;
use rumqttc::v5::mqttbytes::QoS;
use rumqttc::v5::mqttbytes::v5::{Packet, Publish as PublishPacket};
use rumqttc::v5::{AsyncClient, Event, MqttOptions};
use ruststream::runtime::RunningApp;
use ruststream::{
    AckError, Broker, ConnectedBroker, IncomingMessage, OutgoingMessage, PublishPolicy, Publisher,
    Subscriber,
};
use ruststream_rumqttc::{ConnectedMqttBroker, MqttPublish, MqttPublisher};
// The framework loop is a mount site, and a mount site names its broker through this glob.
use ruststream_rumqttc::prelude::*;
use serde::Deserialize;
use tokio::runtime::{Builder, Runtime};
use tokio::sync::{Notify, mpsc, oneshot};
use tokio::task::JoinHandle;
use tokio::time::{sleep, timeout};

// A benchmark measures what ships. With the framework's harness feature compiled in, every
// delivery records what the handler saw and every handler call runs inside a task-local scope, so
// a number taken with it on is not the production path. The benchmark lives in a package of its
// own for the same reason: `ruststream-rumqttc`'s dev-dependencies enable that feature through
// the conformance harness, and a benchmark inside that package would link it.
#[cfg(feature = "testing")]
compile_error!(
    "benchmarks must be built without the `testing` feature; run them through `just bench`"
);

/// Deliveries the probe run takes to measure the raw half's rate.
const PROBE_MESSAGES: usize = 50_000;
/// How long a measured run lasts, at least.
const SECONDS: f64 = 5.0;
/// How much the calibrated count is raised above the probe's estimate.
///
/// The probe is short and cold, so it reads the machine low; without the margin the fastest
/// scenario lands just under the floor.
const MARGIN: f64 = 1.25;
/// The ceiling on a calibrated count, so a machine an order faster does not turn a run into an
/// afternoon.
const MAX_MESSAGES: usize = 2_000_000;
/// Rounds run. Each loop reports its best, median and worst round.
const PAIRS: usize = 3;
/// Round trips the transport probe times before it reports an average.
const PROBE_ROUND_TRIPS: usize = 20_000;
/// Worker threads both halves are driven on.
const WORKERS: usize = 4;

/// How far the publisher may run ahead of the consumer, in messages.
///
/// The ceiling is the broker's, not the harness's taste: MQTT bounds unacknowledged deliveries by
/// the receive maximum this crate announces, and a server holding more than that queues them and
/// eventually drops what does not fit. Staying under the announced 1000 keeps every delivery on
/// the flow-controlled path and out of any queue, and it is far more than either half of a pair
/// is ever behind.
const IN_FLIGHT: usize = 768;
/// How often the publisher checks that ceiling.
const CHECK_EVERY: usize = 64;
/// How long a run may go without a delivery before it is called stuck.
const STALL: Duration = Duration::from_secs(30);
/// How long a connection or a subscription may take before the run is called broken.
const SETUP: Duration = Duration::from_secs(30);

/// The client request channel's capacity, which is what this crate gives its own client.
const CLIENT_CHANNEL: usize = 64;
/// The receive maximum this crate announces, and the bound on unacknowledged deliveries.
const RECEIVE_MAXIMUM: u16 = 1000;
/// The incoming packet ceiling this crate raises the client's default to.
const MAX_PACKET_SIZE: u32 = 1024 * 1024;

/// The body size both halves publish and decode, to the byte: the scenario is published under
/// this number, so the bytes on the wire have to be it.
const BODY_BYTES: usize = 512;
/// How wide one padding value is before the next field starts.
const PAD_WIDTH: usize = 16;
/// The values every body carries. Fixed, so every delivery of a run costs the same.
const ID: u64 = 1_000_000;
const QUANTITY: u32 = 37;

/// What both halves decode a delivery into.
///
/// Two integer fields the handler reads, and a padding the type ignores: a decode that allocates
/// nothing, so the number is about this crate rather than about `serde_json`'s string handling.
#[derive(Debug, Deserialize)]
struct Order {
    id: u64,
    quantity: u32,
}

/// A JSON body carrying the two fields, padded with fields [`Order`] ignores until it is exactly
/// `size` bytes.
///
/// The padding is a run of equally wide fields and one last field cut to whatever is left, so a
/// scenario published as a 512 byte body is one. Building it is startup work, and the assertion
/// below holds the promise the published name makes.
fn json_body(size: usize) -> Vec<u8> {
    let mut body = format!("{{\"id\":{ID},\"quantity\":{QUANTITY}");
    let mut field = 0u32;
    loop {
        let key = format!(",\"f{field}\":\"\"");
        // One byte stays reserved for the closing brace.
        let Some(room) = size.checked_sub(body.len() + key.len() + 1) else {
            break;
        };
        // A full-width field only when what it leaves behind can still hold the next one, whose
        // key is at most one digit longer. Otherwise this is the last field and it takes the
        // rest, because a remainder too small to start a field would come out as a short body.
        let width = if room > PAD_WIDTH + key.len() {
            PAD_WIDTH
        } else {
            room
        };
        body.push_str(&key[..key.len() - 1]);
        body.extend(repeat_n('x', width));
        body.push('"');
        field += 1;
    }
    body.push('}');
    assert_eq!(
        body.len(),
        size,
        "a body has to be the size the scenario publishes"
    );
    body.into_bytes()
}

/// The names one run owns: nothing is shared with the run before it.
///
/// A client identifier is a name here like any other. MQTT gives a session to an identifier, so
/// reusing one would hand a run the session the run before it left behind.
#[derive(Clone, Debug)]
struct Names {
    topic: String,
    consumer: String,
    publisher: String,
    probe: String,
}

impl Names {
    fn fresh() -> Self {
        let stamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|since| since.as_nanos())
            .unwrap_or_default();
        Self {
            topic: format!("ruststream/bench/{stamp}"),
            consumer: format!("rs-bench-sub-{stamp}"),
            publisher: format!("rs-bench-pub-{stamp}"),
            probe: format!("rs-bench-probe-{stamp}"),
        }
    }
}

/// The names the service being built subscribes to.
///
/// `#[subscriber(..)]` takes an expression and evaluates it where the handler is mounted, which is
/// inside the builder of the run that is starting. A run installs its own names here first, so the
/// subscription the framework opens is the one this run publishes to.
static NAMES: Mutex<Option<Names>> = Mutex::new(None);

fn install(names: &Names) {
    *NAMES
        .lock()
        .expect("the names cell is never held across a panic") = Some(names.clone());
}

fn installed() -> Names {
    NAMES
        .lock()
        .expect("the names cell is never held across a panic")
        .clone()
        .expect("a run installs its names before it builds the service")
}

/// Counts deliveries and marks the ends of the measured window.
///
/// Both halves call the same methods, so both pay for the signal. A delivery pays one relaxed
/// increment and two comparisons; the waiter is a single future for the whole run, woken once.
#[derive(Clone, Debug)]
struct Run(Arc<RunInner>);

#[derive(Debug)]
struct RunInner {
    total: usize,
    seen: AtomicUsize,
    first: OnceLock<Instant>,
    last: OnceLock<Instant>,
    drained: Notify,
}

impl Run {
    fn new(total: usize) -> Self {
        Self(Arc::new(RunInner {
            total,
            seen: AtomicUsize::new(0),
            first: OnceLock::new(),
            last: OnceLock::new(),
            drained: Notify::new(),
        }))
    }

    /// Records one handled delivery, and answers whether the run is over.
    fn arrived(&self) -> bool {
        let seen = self.0.seen.fetch_add(1, Ordering::Relaxed) + 1;
        if seen == 1 {
            let _ = self.0.first.set(Instant::now());
        }
        if seen == self.0.total {
            let _ = self.0.last.set(Instant::now());
            self.0.drained.notify_one();
        }
        seen >= self.0.total
    }

    fn handled(&self) -> usize {
        self.0.seen.load(Ordering::Acquire).min(self.0.total)
    }

    /// Resolves once every expected delivery has been handled.
    async fn drained(&self) {
        while self.0.seen.load(Ordering::Acquire) < self.0.total {
            self.0.drained.notified().await;
        }
    }

    /// The measured window: the first delivery to the end of the last handler call.
    fn window(&self) -> Duration {
        let first = *self.0.first.get().expect("the run took a delivery");
        let last = *self.0.last.get().expect("the run took its last delivery");
        last - first
    }
}

/// Waits for the run to finish, and fails with what it was waiting for if it stops moving.
async fn drain(run: &Run, half: &str) {
    let mut seen = 0;
    loop {
        if timeout(STALL, run.drained()).await.is_ok() {
            return;
        }
        let handled = run.handled();
        assert!(
            handled > seen,
            "{half}: {handled} of {} deliveries handled and nothing moved for {STALL:?}",
            run.0.total
        );
        seen = handled;
    }
}

/// What the measured half of one run produced.
#[derive(Clone, Copy, Debug)]
struct Sample {
    window: Duration,
    /// How often the publisher had to wait for the consumer to fall back inside the window.
    throttled: usize,
}

impl Sample {
    fn rate(self, messages: usize) -> f64 {
        messages as f64 / self.window.as_secs_f64()
    }
}

// ---------------------------------------------------------------------------------------------
// The client both halves share
// ---------------------------------------------------------------------------------------------

/// Host and port out of the `mqtt://host:port` the recipe passes.
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

/// The client configuration `MqttBroker` builds for a broker with nothing set on it, spelled out
/// here so the two halves ask the server for the same thing.
fn options(url: &str, id: &str) -> MqttOptions {
    let (host, port) = endpoint(url);
    let mut options = MqttOptions::new(id.to_owned(), host, port);
    options.set_max_packet_size(Some(MAX_PACKET_SIZE));
    options.set_receive_maximum(Some(RECEIVE_MAXIMUM));
    // The crate settles a delivery when the handler returns, so the client must not answer for it
    // on its way in.
    options.set_manual_acks(true);
    options
}

/// What a raw connection is opened for.
///
/// The probe waits for every PUBACK, and a run must not: a publisher of a `QoS` 1 scenario collects
/// one per message, and a channel nobody drains would grow with the run it is measuring.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Purpose {
    Run,
    Probe,
}

/// A client and the task that drives its event loop, which is the shape this crate's own
/// connection has.
struct Raw {
    client: AsyncClient,
    deliveries: Option<mpsc::UnboundedReceiver<PublishPacket>>,
    settled: Option<mpsc::UnboundedReceiver<()>>,
    subscribed: Option<oneshot::Receiver<()>>,
    /// Set before the connection is closed on purpose, so the event loop's last error is read as
    /// the teardown it is rather than reported as a fault on every run.
    closing: Arc<AtomicBool>,
    poller: JoinHandle<()>,
}

impl Raw {
    /// Connects and returns once the broker's CONNACK has arrived.
    async fn connect(url: &str, id: &str, purpose: Purpose) -> Self {
        let (client, mut eventloop) = AsyncClient::new(options(url, id), CLIENT_CHANNEL);
        let (deliveries_tx, deliveries) = mpsc::unbounded_channel();
        let (settled_tx, settled) = mpsc::unbounded_channel();
        let (connected_tx, connected) = oneshot::channel();
        let (subscribed_tx, subscribed) = oneshot::channel();
        let closing = Arc::new(AtomicBool::new(false));
        let poller = tokio::spawn({
            let closing = Arc::clone(&closing);
            async move {
                let mut connected = Some(connected_tx);
                let mut subscribed = Some(subscribed_tx);
                let settled_tx = (purpose == Purpose::Probe).then_some(settled_tx);
                loop {
                    match eventloop.poll().await {
                        Ok(Event::Incoming(Packet::Publish(publish))) => {
                            if deliveries_tx.send(publish).is_err() {
                                break;
                            }
                        }
                        Ok(Event::Incoming(Packet::PubAck(_))) => {
                            if let Some(settled_tx) = &settled_tx {
                                let _ = settled_tx.send(());
                            }
                        }
                        Ok(Event::Incoming(Packet::ConnAck(_))) => {
                            if let Some(done) = connected.take() {
                                let _ = done.send(());
                            }
                        }
                        Ok(Event::Incoming(Packet::SubAck(_))) => {
                            if let Some(done) = subscribed.take() {
                                let _ = done.send(());
                            }
                        }
                        Ok(_) => {}
                        Err(err) => {
                            if !closing.load(Ordering::Relaxed) {
                                // The run's own stall detector turns this into a failure naming how
                                // far it got, which says more than the error on its own.
                                eprintln!("the mqtt event loop stopped: {err}");
                            }
                            break;
                        }
                    }
                }
            }
        });
        timeout(SETUP, connected)
            .await
            .expect("the broker answers the connection")
            .expect("the event loop reaches the broker's CONNACK");
        Self {
            client,
            deliveries: Some(deliveries),
            settled: (purpose == Purpose::Probe).then_some(settled),
            subscribed: Some(subscribed),
            closing,
            poller,
        }
    }

    /// The channel the event loop reports every PUBACK over, taken by the probe.
    fn settled(&mut self) -> mpsc::UnboundedReceiver<()> {
        self.settled
            .take()
            .expect("only a probe connection reports its acknowledgements")
    }

    /// The channel the event loop hands deliveries over, taken by the task that consumes them.
    fn deliveries(&mut self) -> mpsc::UnboundedReceiver<PublishPacket> {
        self.deliveries
            .take()
            .expect("a raw client is consumed once")
    }

    /// Opens the subscription and returns once the broker has granted it, which is what puts the
    /// consumer in place before the first message is published.
    async fn subscribe(&mut self, topic: &str, qos: QoS) {
        let subscribed = self
            .subscribed
            .take()
            .expect("a raw client subscribes once");
        self.client
            .subscribe(topic.to_owned(), qos)
            .await
            .expect("the broker accepts the subscription");
        timeout(SETUP, subscribed)
            .await
            .expect("the broker answers the subscription")
            .expect("the event loop reaches the broker's SUBACK");
    }

    /// Closes the connection, so a run leaves the broker nothing of its own behind.
    async fn stop(self) {
        self.closing.store(true, Ordering::Relaxed);
        let _ = self.client.disconnect().await;
        drop(self.client);
        drop(self.deliveries);
        drop(self.settled);
        // A loop that has not noticed yet is not worth waiting on: the client it reads is gone
        // and the run it belonged to is over.
        let _ = timeout(Duration::from_secs(2), self.poller).await;
    }
}

/// Holds the publisher back while the consumer is further behind than [`IN_FLIGHT`].
///
/// Counts every wait, so a run can say whether the window ever bound it.
async fn throttle(sent: usize, run: &Run, throttled: &mut usize) {
    if !sent.is_multiple_of(CHECK_EVERY) {
        return;
    }
    while sent.saturating_sub(run.handled()) > IN_FLIGHT {
        *throttled += 1;
        sleep(Duration::from_micros(200)).await;
    }
}

/// Publishes the run's bodies through the client this crate wraps.
async fn publish_through_client(
    client: &AsyncClient,
    topic: &str,
    qos: QoS,
    messages: usize,
    run: &Run,
) -> usize {
    let body = Bytes::from(json_body(BODY_BYTES));
    let mut throttled = 0;
    for sent in 0..messages {
        throttle(sent, run, &mut throttled).await;
        client
            .publish(topic.to_owned(), qos, false, body.clone())
            .await
            .expect("the broker accepts the publish");
    }
    throttled
}

/// Publishes the same bodies through this crate's own publisher, which is what a service does.
async fn publish_through_crate(
    publisher: &MqttPublisher,
    topic: &str,
    messages: usize,
    run: &Run,
) -> usize {
    let body = json_body(BODY_BYTES);
    let mut throttled = 0;
    for sent in 0..messages {
        throttle(sent, run, &mut throttled).await;
        publisher
            .publish(OutgoingMessage::new(topic, &body), None)
            .await
            .expect("the broker accepts the publish");
    }
    throttled
}

// ---------------------------------------------------------------------------------------------
// The measured half: this crate's own consumer and publisher
// ---------------------------------------------------------------------------------------------

/// The feeding side of the two crate-driven loops: a second connection and the publisher of the
/// scenario's quality of service.
async fn feed(
    url: &str,
    names: &Names,
    scenario: Scenario,
) -> (ConnectedMqttBroker, MqttPublisher) {
    let feeder = MqttBroker::new(url, names.publisher.clone())
        .connect()
        .await
        .expect("the broker connects");
    let publisher = MqttPublish::default()
        .qos(scenario.declared_qos())
        .pair(&feeder)
        .await
        .expect("the publisher pairs with the connection");
    (feeder, publisher)
}

/// A loop over the subscription this crate opens, settling every delivery where the raw loop
/// settles it.
///
/// No service is started: what is measured here is the descriptor, the stream, the message and
/// the acknowledgement this crate ships, and nothing above them.
async fn adapter(scenario: Scenario, url: &str, names: &Names, messages: usize) -> Sample {
    let connected = MqttBroker::new(url, names.consumer.clone())
        .connect()
        .await
        .expect("the broker connects");
    let subscriber = connected
        .subscribe_topic(MqttTopic::new(names.topic.clone()).qos(scenario.declared_qos()))
        .await
        .expect("the subscription opens");

    let run = Run::new(messages);
    let consuming = tokio::spawn({
        let run = run.clone();
        let mut subscriber = subscriber;
        async move {
            let mut stream = pin!(subscriber.stream());
            while let Some(delivery) = stream.next().await {
                let message = delivery.expect("the subscription delivers");
                let order: Order =
                    serde_json::from_slice(message.payload()).expect("the body decodes");
                black_box((order.id, order.quantity));
                let done = run.arrived();
                // At `QoS` 0 the message reports that there is nothing to acknowledge instead of
                // pretending it settled one, which is the same answer the raw half acts on.
                match message.ack().await {
                    Ok(()) | Err(AckError::Unsupported) => {}
                    Err(err) => panic!("the acknowledgement failed: {err}"),
                }
                if done {
                    break;
                }
            }
        }
    });

    let (feeder, publisher) = feed(url, names, scenario).await;
    let throttled = publish_through_crate(&publisher, &names.topic, messages, &run).await;
    drain(&run, "adapter").await;
    consuming.await.expect("the consuming task ends");

    feeder.shutdown().await.expect("the connection closes");
    connected.shutdown().await.expect("the connection closes");
    Sample {
        window: run.window(),
        throttled,
    }
}

// ---------------------------------------------------------------------------------------------
// The framework loop: the service a user writes
// ---------------------------------------------------------------------------------------------

#[subscriber(MqttTopic::new(installed().topic).qos(Qos::AtMostOnce))]
async fn at_most_once(order: &Order, ctx: &mut Context<'_, (), Run>) -> HandlerOutcome {
    black_box((order.id, order.quantity));
    ctx.state().arrived();
    HandlerOutcome::ack()
}

#[subscriber(MqttTopic::new(installed().topic).qos(Qos::AtLeastOnce))]
async fn at_least_once(order: &Order, ctx: &mut Context<'_, (), Run>) -> HandlerOutcome {
    black_box((order.id, order.quantity));
    ctx.state().arrived();
    HandlerOutcome::ack()
}

async fn start_at_most_once(url: &str, names: &Names, run: Run) -> RunningApp {
    RustStream::new(AppInfo::new("mqtt-bench", "0.0.0"))
        .on_startup(async move |()| Ok::<_, Infallible>(run))
        .with_broker(MqttBroker::new(url, names.consumer.clone()), |b| {
            b.include(at_most_once);
        })
        .start()
        .await
        .expect("the service starts")
}

async fn start_at_least_once(url: &str, names: &Names, run: Run) -> RunningApp {
    RustStream::new(AppInfo::new("mqtt-bench", "0.0.0"))
        .on_startup(async move |()| Ok::<_, Infallible>(run))
        .with_broker(MqttBroker::new(url, names.consumer.clone()), |b| {
            b.include(at_least_once);
        })
        .start()
        .await
        .expect("the service starts")
}

/// The whole service: the runtime opens the subscription, decodes into [`Order`], calls the
/// handler and settles what it returns.
async fn framework(scenario: Scenario, url: &str, names: &Names, messages: usize) -> Sample {
    let run = Run::new(messages);
    install(names);
    let app = match scenario {
        Scenario::AtMostOnce => start_at_most_once(url, names, run.clone()).await,
        Scenario::AtLeastOnce => start_at_least_once(url, names, run.clone()).await,
    };

    let (feeder, publisher) = feed(url, names, scenario).await;
    let throttled = publish_through_crate(&publisher, &names.topic, messages, &run).await;
    drain(&run, "framework").await;

    app.shutdown().await.expect("the service stops");
    feeder.shutdown().await.expect("the connection closes");
    Sample {
        window: run.window(),
        throttled,
    }
}

// ---------------------------------------------------------------------------------------------
// The hand-written half
// ---------------------------------------------------------------------------------------------

async fn raw(scenario: Scenario, url: &str, names: &Names, messages: usize) -> Sample {
    let qos = scenario.wire_qos();
    let mut subscriber = Raw::connect(url, &names.consumer, Purpose::Run).await;
    subscriber.subscribe(&names.topic, qos).await;

    let run = Run::new(messages);
    let consuming = tokio::spawn({
        let run = run.clone();
        let client = subscriber.client.clone();
        let mut deliveries = subscriber.deliveries();
        async move {
            while let Some(publish) = deliveries.recv().await {
                let order: Order =
                    serde_json::from_slice(&publish.payload).expect("the body decodes");
                black_box((order.id, order.quantity));
                // The framework acknowledges once the handler is done, so the window closes
                // before the acknowledgement on this half too. At QoS 0 there is nothing to
                // acknowledge, and the message this crate hands a handler reports as much.
                let done = run.arrived();
                if qos != QoS::AtMostOnce {
                    client
                        .ack(&publish)
                        .await
                        .expect("the acknowledgement reaches the broker");
                }
                if done {
                    break;
                }
            }
        }
    });

    let publisher = Raw::connect(url, &names.publisher, Purpose::Run).await;
    let throttled =
        publish_through_client(&publisher.client, &names.topic, qos, messages, &run).await;
    drain(&run, "raw").await;
    consuming.await.expect("the consuming task ends");

    publisher.stop().await;
    subscriber.stop().await;
    Sample {
        window: run.window(),
        throttled,
    }
}

// ---------------------------------------------------------------------------------------------
// The transport probe
// ---------------------------------------------------------------------------------------------

/// What one round trip against this broker costs, measured outside both halves.
///
/// A `QoS` 1 publish whose PUBACK the client waits for, one at a time on a connection of its own:
/// the cheapest exchange this protocol answers, and the same two packets a `QoS` 1 delivery settles
/// with. Nothing subscribes to the topic, so what is timed is the broker's turnaround and the
/// loopback rather than a delivery.
///
/// The first exchange is thrown away: it pays for a cold connection and for a topic the broker
/// has not seen before.
async fn round_trip(url: &str, names: &Names) -> Duration {
    let mut probe = Raw::connect(url, &names.probe, Purpose::Probe).await;
    let mut settled = probe.settled();
    let topic = format!("{}/probe", names.topic);
    let body = Bytes::from_static(b"probe");

    let mut exchange = async |client: &AsyncClient| {
        client
            .publish(topic.clone(), QoS::AtLeastOnce, false, body.clone())
            .await
            .expect("the broker accepts the probe publish");
        timeout(SETUP, settled.recv())
            .await
            .expect("the broker answers the probe publish")
            .expect("the probe connection stays open");
    };

    exchange(&probe.client).await;
    let started = Instant::now();
    for _ in 0..PROBE_ROUND_TRIPS {
        exchange(&probe.client).await;
    }
    let elapsed = started.elapsed();

    probe.stop().await;
    elapsed / PROBE_ROUND_TRIPS as u32
}

// ---------------------------------------------------------------------------------------------
// The run
// ---------------------------------------------------------------------------------------------

/// The quality of service a scenario runs at, which is the setting that decides everything here.
///
/// Both halves publish and subscribe at it: MQTT delivers at the lower of the two, so a
/// subscription that names one and a publish that names another would measure the other one.
#[derive(Clone, Copy, Debug)]
enum Scenario {
    AtMostOnce,
    AtLeastOnce,
}

impl Scenario {
    const fn name(self) -> &'static str {
        match self {
            Self::AtMostOnce => "QoS 0 topic, 512 B JSON",
            Self::AtLeastOnce => "QoS 1 topic, 512 B JSON, ack each",
        }
    }

    /// What the raw half names on the wire.
    const fn wire_qos(self) -> QoS {
        match self {
            Self::AtMostOnce => QoS::AtMostOnce,
            Self::AtLeastOnce => QoS::AtLeastOnce,
        }
    }

    /// What the measured half declares on the descriptor and on the publish policy.
    const fn declared_qos(self) -> Qos {
        match self {
            Self::AtMostOnce => Qos::AtMostOnce,
            Self::AtLeastOnce => Qos::AtLeastOnce,
        }
    }

    /// How many round trips of the transport one delivery costs the consumer.
    ///
    /// At `QoS` 1 the consumer answers every delivery with a PUBACK, which closes the exchange the
    /// broker's PUBLISH opened: one round trip. At `QoS` 0 nothing travels back, so a delivery
    /// costs none and the row is never broker-bound however slow the broker is.
    const fn round_trips_per_delivery(self) -> u32 {
        match self {
            Self::AtMostOnce => 0,
            Self::AtLeastOnce => 1,
        }
    }
}

/// Best, median and worst of the rounds.
///
/// Noise on the machine only ever slows a run down, so the fastest round is the closest to the
/// undisturbed cost, the median is the typical one, and the slowest says how far from quiet the
/// machine was.
#[derive(Clone, Copy, Debug)]
struct Stats {
    best: f64,
    median: f64,
    worst: f64,
}

impl Stats {
    fn of(rates: &[f64]) -> Self {
        assert!(!rates.is_empty(), "no round was run");
        let mut sorted = rates.to_vec();
        sorted.sort_by(f64::total_cmp);
        let middle = sorted.len() / 2;
        let median = if sorted.len() % 2 == 1 {
            sorted[middle]
        } else {
            f64::midpoint(sorted[middle - 1], sorted[middle])
        };
        Self {
            best: sorted[sorted.len() - 1],
            median,
            worst: sorted[0],
        }
    }

    fn spread(self) -> f64 {
        self.best - self.worst
    }
}

#[derive(Debug)]
struct Measured {
    scenario: Scenario,
    messages: usize,
    pairs: usize,
    raw: Stats,
    adapter: Stats,
    framework: Stats,
    adapter_overhead_percent: f64,
    overhead_percent: f64,
    adapter_verdict: &'static str,
    verdict: &'static str,
    broker_bound: bool,
    throttled: usize,
}

async fn measure(
    scenario: Scenario,
    url: &str,
    pairs: usize,
    seconds: f64,
    round_trip: Duration,
) -> Measured {
    // The probe is the warm-up as well: its result is thrown away, and the rate it measured sets
    // a count that makes every run below last at least `seconds`.
    let probe = raw(scenario, url, &Names::fresh(), PROBE_MESSAGES).await;
    let messages = ((probe.rate(PROBE_MESSAGES) * seconds * MARGIN) as usize)
        .clamp(PROBE_MESSAGES, MAX_MESSAGES);
    println!(
        "{}: {messages} messages per run ({:.0} msg/s probed)",
        scenario.name(),
        probe.rate(PROBE_MESSAGES)
    );

    let mut raws = Vec::with_capacity(pairs);
    let mut adapters = Vec::with_capacity(pairs);
    let mut frameworks = Vec::with_capacity(pairs);
    let mut throttled = 0;
    for round in 1..=pairs {
        let measured_raw = raw(scenario, url, &Names::fresh(), messages).await;
        let measured_adapter = adapter(scenario, url, &Names::fresh(), messages).await;
        let measured_framework = framework(scenario, url, &Names::fresh(), messages).await;
        println!(
            "  round {round:>2}: raw {:>9.0}, adapter {:>9.0}, framework {:>9.0} msg/s",
            measured_raw.rate(messages),
            measured_adapter.rate(messages),
            measured_framework.rate(messages)
        );
        raws.push(measured_raw.rate(messages));
        adapters.push(measured_adapter.rate(messages));
        frameworks.push(measured_framework.rate(messages));
        throttled +=
            measured_raw.throttled + measured_adapter.throttled + measured_framework.throttled;
    }

    let raw_stats = Stats::of(&raws);
    let adapter_stats = Stats::of(&adapters);
    let framework_stats = Stats::of(&frameworks);
    // The verdict is about the headline comparison, the whole service against the raw client. The
    // adapter column carries its own percentage and the page applies the same rule to it from the
    // spreads the document publishes.
    let noise = raw_stats.spread().max(framework_stats.spread());
    let difference = (raw_stats.best - framework_stats.best).abs();
    let adapter_noise = raw_stats.spread().max(adapter_stats.spread());
    let adapter_difference = (raw_stats.best - adapter_stats.best).abs();
    // What the transport charges the consumer per delivery, against what a delivery took. Half is
    // the line: above it the raw client spent most of the run waiting on the socket, and the
    // framework did its work inside a wait that was being paid anyway.
    let per_message = 1.0 / raw_stats.best;
    let charged = round_trip.as_secs_f64() * f64::from(scenario.round_trips_per_delivery());
    println!(
        "  {} round trip(s) per delivery at {:.0} us against {:.2} us per message",
        scenario.round_trips_per_delivery(),
        round_trip.as_secs_f64() * 1e6,
        per_message * 1e6
    );
    Measured {
        scenario,
        messages,
        pairs,
        raw: raw_stats,
        adapter: adapter_stats,
        framework: framework_stats,
        adapter_overhead_percent: (raw_stats.best - adapter_stats.best) / raw_stats.best * 100.0,
        overhead_percent: (raw_stats.best - framework_stats.best) / raw_stats.best * 100.0,
        adapter_verdict: if adapter_difference < adapter_noise {
            "indistinguishable"
        } else {
            "measured"
        },
        verdict: if difference < noise {
            "indistinguishable"
        } else {
            "measured"
        },
        broker_bound: charged >= per_message / 2.0,
        throttled,
    }
}

fn document(measured: &[Measured], round_trip: Duration) -> String {
    let mut out = format!(
        concat!(
            "{{\n",
            "  \"round_trip_us\": {round_trip:.1},\n",
            "  \"round_trip_samples\": {samples},\n",
            "  \"scenarios\": [\n",
        ),
        round_trip = round_trip.as_secs_f64() * 1e6,
        samples = PROBE_ROUND_TRIPS,
    );
    for (index, row) in measured.iter().enumerate() {
        let comma = if index + 1 == measured.len() { "" } else { "," };
        write!(
            out,
            concat!(
                "    {{\n",
                "      \"name\": \"{name}\",\n",
                "      \"unit\": \"msg/s\",\n",
                "      \"messages\": {messages},\n",
                "      \"pairs\": {pairs},\n",
                "      \"raw\": {{ \"best\": {raw_best:.0}, \"median\": {raw_median:.0}, \"worst\": {raw_worst:.0} }},\n",
                "      \"adapter\": {{ \"best\": {ad_best:.0}, \"median\": {ad_median:.0}, \"worst\": {ad_worst:.0} }},\n",
                "      \"framework\": {{ \"best\": {fw_best:.0}, \"median\": {fw_median:.0}, \"worst\": {fw_worst:.0} }},\n",
                "      \"adapter_overhead_percent\": {adapter_overhead:.1},\n",
                "      \"adapter_verdict\": \"{adapter_verdict}\",\n",
                "      \"overhead_percent\": {overhead:.1},\n",
                "      \"verdict\": \"{verdict}\",\n",
                "      \"broker_bound\": {broker_bound}\n",
                "    }}{comma}\n",
            ),
            name = row.scenario.name(),
            messages = row.messages,
            pairs = row.pairs,
            raw_best = row.raw.best,
            raw_median = row.raw.median,
            raw_worst = row.raw.worst,
            ad_best = row.adapter.best,
            ad_median = row.adapter.median,
            ad_worst = row.adapter.worst,
            fw_best = row.framework.best,
            fw_median = row.framework.median,
            fw_worst = row.framework.worst,
            adapter_overhead = row.adapter_overhead_percent,
            adapter_verdict = row.adapter_verdict,
            overhead = row.overhead_percent,
            verdict = row.verdict,
            broker_bound = row.broker_bound,
            comma = comma,
        )
        .expect("writing to a String");
    }
    out.push_str("  ]\n}\n");
    out
}

fn runtime() -> Runtime {
    Builder::new_multi_thread()
        .worker_threads(WORKERS)
        .enable_all()
        .build()
        .expect("the tokio runtime builds")
}

/// A positive count from the environment, or the default.
///
/// The parse target rejects zero, so a pairs count of zero is refused here rather than after the
/// probe run, where it would panic in the statistics with no round to report.
fn number(name: &str, fallback: usize) -> usize {
    env::var(name).ok().map_or(fallback, |value| {
        value
            .parse::<NonZeroUsize>()
            .unwrap_or_else(|_| panic!("{name} must be a positive number"))
            .get()
    })
}

fn main() {
    let url = env::var("MQTT_TEST_URL")
        .expect("MQTT_TEST_URL names the broker to measure against; `just bench` sets it");
    let pairs = number("RUSTSTREAM_BENCH_PAIRS", PAIRS);
    let seconds = number("RUSTSTREAM_BENCH_SECONDS", SECONDS as usize) as f64;
    let out = env::var("RUSTSTREAM_BENCH_OUT").unwrap_or_else(|_| "bench-paired.json".to_owned());

    let runtime = runtime();
    // Outside both halves and before either scenario: what the flag is decided against does not
    // belong to a run, and a run must not pay for it.
    let round_trip = runtime.block_on(round_trip(&url, &Names::fresh()));
    println!(
        "round trip: {:.1} us over {PROBE_ROUND_TRIPS} exchanges",
        round_trip.as_secs_f64() * 1e6
    );
    let measured: Vec<Measured> = [Scenario::AtMostOnce, Scenario::AtLeastOnce]
        .into_iter()
        .map(|scenario| runtime.block_on(measure(scenario, &url, pairs, seconds, round_trip)))
        .collect();

    println!();
    for row in &measured {
        println!(
            "{}: raw {:.0}, adapter {:.0} ({:+.1}%), framework {:.0} ({:+.1}%) msg/s ({}{}), \
             publisher held back {} times",
            row.scenario.name(),
            row.raw.best,
            row.adapter.best,
            row.adapter_overhead_percent,
            row.framework.best,
            row.overhead_percent,
            row.verdict,
            if row.broker_bound {
                ", broker-bound"
            } else {
                ""
            },
            row.throttled,
        );
    }

    std::fs::write(&out, document(&measured, round_trip)).expect("the summary is written");
    println!("\nwrote {out}");
}
