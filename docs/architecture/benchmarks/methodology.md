---
title: Methodology
description: "How ruststream-rumqttc measures itself: the three loops, the run on a real MQTT server, the broker-bound arithmetic and the code count."
---

# Methodology

How `ruststream-rumqttc` measures itself: the three loops, the run on a real MQTT server, the
broker-bound arithmetic and the code count. The procedure is the framework's own
([RustStream methodology](https://powersemmi.github.io/ruststream/latest/benchmarks/#methodology)),
and this page states what it is on MQTT.

## The three loops

One process runs each scenario three times over, as three loops that differ in one thing: what
carries the messages.

- **Raw client.** `rumqttc` driven directly: its event loop polled on a task of its own, which
  hands each delivery to the consumer over a channel, as this crate does. The consumer decodes it
  and, at QoS 1, acknowledges it.
- **Broker crate.** This crate and nothing above it: the broker connects, the
  [descriptor](../../components/subscriptions/index.md) opens a subscription, and a loop pulls
  deliveries off its stream, decodes them and acks them.
- **RustStream.** The service a user writes: a `#[subscriber]` handler and the app around it.

Everything else is equal across the three: the connection options (manual acknowledgements, a
Receive Maximum of 1000, a 1 MiB packet ceiling), the quality of service of the subscription and
of the publish, the place of the ack, the decode into the same struct, the payload bytes, the tokio
runtime with four worker threads, and the binary.

The body is 512 bytes of JSON. It decodes into a struct of two integer fields, so the decode
allocates nothing and the number is about this crate rather than about the JSON parser.

## The run

- **A fresh topic and fresh client ids per run.** A run never sees what an earlier one left.
- **The consumer subscribes first.** A publisher on a second connection then feeds it, and stays at
  most 768 messages ahead. At QoS 1 that keeps every delivery inside the Receive Maximum window of
  1000; at QoS 0 it keeps the publisher from running away.
- **The window opens at the first delivery and closes when the last one is decoded**, before its
  acknowledgement leaves, in all three loops. Connecting and subscribing are outside it.
- **The message count comes from a probe**, which measures the raw loop's rate on 50000 messages,
  so a measured run lasts at least five seconds on any machine.
- **The loops are interleaved**: raw, crate, RustStream, three rounds. Each loop reports its best,
  median and worst round.
- **QoS 2 is not measured.** Its four-packet handshake makes the server's bookkeeping the subject.

A row is `broker-bound` when the consumer spent most of the run waiting on the server. That is
arithmetic: a probe times 20000 QoS 1 publishes whose PUBACK it waits for, on a connection of its
own, and takes the mean. The transport states how many round trips one delivery costs, and the row
is flagged when their product reaches half the measured time per message. A QoS 0 delivery costs
no round trip, because nothing travels back. A QoS 1 delivery costs one, the PUBACK. The round trip
is published with the machine, so the arithmetic can be checked.

## The code count

Each scenario of the [cost chart](index.md#cost-of-the-code) is the service a user writes, on
[`MqttBroker`](../../components/broker/index.md) against the same server, built without the
`testing` feature, on a single-threaded runtime. A feeder on a thread of its own publishes the
run's messages at QoS 1 before the measured region opens. The server holds them for the session,
and the region drains them all.

Instructions come from callgrind and allocations from DHAT. A scenario runs over one delivery,
then 500, then 1000. The difference between the last two is the steady cost of a message, and the
single delivery is the cold start. 500 is the most a run takes: twice that fills the Receive
Maximum of 1000.

The server is real, so a count moves a little between runs: how the socket hands the client its
bytes depends on timing. A run fails when a scenario allocates more than the floor it declares,
its cold start plus its steady cost per delivery. Against a saved baseline it also fails on two
percent more instructions in a scenario.

## Running it yourself

```bash
just bench
```

The recipe starts the stand from `docker-compose.test.yml`, runs both comparison scenarios, stops
the stand and rewrites `docs/benchmarks/results.json`. It takes a few minutes and wants the machine
to itself. It clears `RUSTFLAGS`, so the binary is not tied to the CPU it was built on.
Both recipes build the benchmarks the way a release is built, with fat link-time optimization
across crates, so the numbers describe the code a service ships.

```bash
just bench-code
```

The code count takes about a minute and needs valgrind. It installs the benchmark runner at the
release `Cargo.lock` pins. `just bench-code --save-baseline=main` on `main` records a baseline,
and `just bench-code --baseline=main` on a branch compares with it. A leading number counts over
fewer deliveries (`just bench-code 250`); the published chart is counted at 500.
