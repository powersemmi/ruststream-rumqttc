---
title: Numbers
description: "What the benchmark numbers of ruststream-rumqttc measure, and what they do not allow you to conclude."
---

# Numbers

What the benchmark numbers of `ruststream-rumqttc` measure, and what they do not allow you to
conclude.

## The comparison against a raw client

The three loops of a row are comparable with each other, because they ran in one process,
interleaved, on one server. A row is not comparable with a row another broker crate publishes: the
transports do different work per message.

The server runs on the same machine, in a container on the loopback. With no network latency, the
same cost per message is a larger share of the time than it would be against a server across a
network. Read the percentage as an upper bound on what a deployed service pays.

A row reported as `indistinguishable` is one whose loops differ by less than the spread between
their rounds. A figure below that noise would read as precision that was never measured, so none is
published.

A `broker-bound` row is one where the consumer waited on the server for most of the run. The work
of the crate and the runtime then happens inside that wait, and the difference comes out near zero.
That is a lower bound on the cost, not a measurement of it. A QoS 1 delivery costs one round trip
and a QoS 0 delivery none, so the QoS 1 row is the one the mark can land on.

## The cost of the code

An instruction count holds this crate's code, the runtime above it and the `rumqttc` work on the
service's thread: the event loop's reads, the packet decode, the dispatch and the PUBACK. It moves
a little between runs, because the server is real. The same count costs more time where it misses
the cache, so a count is not a time.

Every figure is the steady state, the difference between a run of 500 deliveries and one of 1000.
Connecting, subscribing and the first allocations behind them are the cold start, paid once.

## What they do not mean

A row is one subscription reading one topic, a small body and a single server. It measures what a
delivery costs inside this crate, not what MQTT can carry.

A share group, overlapping filters, a pool of worker lanes and QoS 2 each answer a different
question, and the comparison measures none of them. The code chart counts a
[batch handler](../../components/subscriptions/batches.md) and a reply on the same stand.

The numbers are a snapshot of one machine on one day, with the build recorded beside them. They are
measured again by hand, on a machine given to the run alone: the differences they show are smaller
than the noise of a shared one.
