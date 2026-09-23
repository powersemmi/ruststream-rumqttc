# Benchmarks

Between the `rumqttc` client and your code this crate does work on every message: the subscription
it opens, the stream it yields, the message it hands over, the acknowledgement it sends, the packet
it publishes. This page says how much, measured against the same work written by hand on the client.

Three loops in one process run the same scenario, and they differ in one thing each: what carries
the messages. **Raw client** drives `rumqttc` directly. **This crate** drives its own broker,
subscription, message and publisher from a loop written in the benchmark, with no handler and no
runtime above it. **RustStream service** is the whole thing a user writes: a `#[subscriber]` handler
on an app the runtime starts.

The two differences answer two questions. *Crate overhead* is what this crate's own consumer and
publisher cost over the client they wrap, which is what this repository is responsible for.
*Service overhead* is the whole service against the same raw client, so the distance between the two
is what the runtime costs on top of this broker in particular. That second one is worth reading per
broker: if adapters are thin everywhere and the runtime's share still differs, the difference lives
in how the two meet.

Everything else is held equal - the connection options, the quality of service on both the
subscription and the publish, the position of the acknowledgement, the decode into the same type,
the payload bytes, the tokio runtime and the build. The procedure is the framework's own and is
described on the
[RustStream benchmarks page](https://powersemmi.github.io/ruststream/latest/benchmarks/#methodology);
this page publishes what it produced here.

`rumqttc` hands the caller an event loop to drive, and where that loop runs decides what a delivery
costs. This crate polls it on a task of its own and passes deliveries to the subscription over a
channel, so the raw loop is built the same way. A loop polled from inside the consuming task would
be a different concurrency shape rather than a different consumer.

## The numbers

The best of three interleaved rounds, with the median round in parentheses. Higher is better.

<div id="benchmark-results" data-benchmark-labels='{"loading": "Loading the published results...", "scenario": "Scenario", "raw": "Raw client", "adapter": "This crate", "framework": "RustStream service", "adapterOverhead": "Crate overhead", "overhead": "Service overhead", "indistinguishable": "indistinguishable", "brokerBound": "broker-bound", "machine": "Machine", "os": "OS", "broker": "Broker", "roundTrip": "Round trip", "build": "Build", "versions": "Versions", "measured": "Measured", "instructions": "Instructions per message", "allocations": "Allocations per message", "cold": "Cold start (instructions / allocations)", "unavailable": "No results could be read. They are published at {url}.", "unknownSchema": "The published results declare schema {schema}, which this page does not render."}'></div>

The table is read in your browser from the document the last run wrote, so nothing on this page is a
copy that could have gone stale.

The quality of service separates the two rows, and it decides the `broker-bound` mark as well. At
QoS 0 the broker settles nothing: a delivery is a topic match and a body, and nothing travels back.
At QoS 1 the consumer answers every delivery, which is one round trip of the transport per message.

A column reported as `indistinguishable` is one that differs from the raw client by less than the
spread between runs of either. A figure below the run-to-run noise would read as precision that was never measured,
so none is published.

The `broker-bound` mark is decided by measurement, not by inference. A probe outside every loop
times one round trip against the same broker - a QoS 1 publish whose acknowledgement it waits for -
and a row is marked when the round trips one delivery costs the consumer are worth at least half of
the time a message took. The round trip is published with the machine below, so the arithmetic can
be redone. Where the mark lands, the raw client spent most of the run waiting on the socket and this
crate did its own work inside a wait that was being paid anyway; the row is then a lower bound on
that work rather than a measurement of it.

The machine-readable form of the same run, which the framework's site reads to build its
cross-broker table, is at
[`benchmarks/results.json`](https://powersemmi.github.io/ruststream-rumqttc/latest/benchmarks/results.json).

## The crate's own code

<div id="benchmark-code"></div>

The second table is what a message costs on the service's thread, counted rather than timed:
instructions under callgrind and allocations under DHAT. Each scenario is the service a user
writes, started on `MqttBroker` against the mosquitto of the compose stand, every delivery at
QoS 1. The crate drives the `rumqttc` event loop from a task of the service's runtime, so the count
covers the framework, this crate and the client's work alike: the packet decode, the dispatch, the
`PUBACK`, and for the reply the PUBLISH it sends. Waiting on the socket is not in it, since valgrind
counts instructions and not time.

Instructions and allocations are per message in the steady state: the slope between a run of 500
deliveries and a run of 1000, fed ahead of each run by a client of its own that is never counted.
The last column is what starting the service - the connect, the subscription and the first
delivery - cost once. The numbers are absolute; the core publishes the framework's own cost on its
[benchmarks page](https://powersemmi.github.io/ruststream/latest/benchmarks/).

Three runs of one binary agree within a third of a percent on instructions and within a few
allocations in four thousand, which is how the socket's reads split. `just bench-code` fails on an
allocation above the floor a scenario declares - the highest count of three runs plus one percent -
and with `--baseline=main` on more than two percent more instructions, and a pull request that
changes the cost cites its numbers.

## The machine

<div id="benchmark-environment"></div>

The build flags are published with the numbers because they change them: a binary built with
`-C target-cpu=native` produces a figure no other machine can reproduce, so the recipe clears the
variable before it builds.

## What they do not mean

This is one subscription, one topic, a small body and a broker on the loopback. It measures what a
delivery costs in this crate, not what MQTT can carry, and a row here is not comparable with a row
published for another broker: the transports do different work per message.

All three loops run the client this crate configures - manual acknowledgements, a `Receive Maximum`
of 1000 and a 1 MiB ceiling on an incoming packet - and each feeds itself on a second connection of
its own. The two crate-driven loops feed through this crate's publisher, because that is the path a
service publishes on; what separates them is the dispatch on the consuming side and nothing else. The publisher is never allowed more than 768 messages
ahead of the consumer. At QoS 1 that keeps every delivery inside the window the protocol's own flow
control counts, instead of a queue inside the broker where a server drops what does not fit; at
QoS 0, where nothing is settled, it only stops the publisher running away. At that depth the
consumer is still never starved.

The window a run measures opens at the first delivery and closes once the last one has been decoded
and read, before its acknowledgement leaves, on all three loops alike. One acknowledgement out of the
hundreds of thousands a run carries therefore sits outside every number here.

QoS 2 is not measured. Its four-way handshake makes the broker's bookkeeping the subject, and a
figure taken there would describe mosquitto rather than this crate.

The numbers are a snapshot of one machine on one day. They are re-measured by hand, on a machine
given to the run alone: the difference this page is about is smaller than the noise of a shared one.

## Running it yourself

```bash
just bench
```

The recipe starts the stand from `docker-compose.test.yml`, runs both scenarios, stops the stand and
rewrites `docs/benchmarks/results.json` with what it measured. It wants the machine to itself. The
message count is not fixed: a probe run sets it so that every measured run lasts at least five
seconds on whatever machine it is taken on.

```bash
just bench-code
```

The recipe starts the same stand, counts the code table under valgrind, stops the stand and rewrites
the `code` section of the same document. It takes about a minute and needs valgrind and the
benchmark runner: `cargo install --locked gungraun-runner --version =0.19.4`.
