---
title: Benchmarks
description: "What ruststream-rumqttc costs on every message: throughput against the raw rumqttc client, and instructions and allocations of its code."
---

import {Environment, MovedSections, Results} from '@site/src/components/Benchmarks';
import {mqtt} from '@site/src/components/Benchmarks/mqtt';

export const labels = {"loading": "Loading the results...", "scenario": "Scenario", "raw": "Raw client", "adapter": "Broker crate", "framework": "RustStream", "overhead": "Overhead", "against": "({percent} over the client)", "indistinguishable": "indistinguishable", "brokerBound": "broker-bound", "measured": "measured", "details": "How the numbers were taken", "pending": "No results published yet: {brokers}.", "instructions": "Instructions", "allocations": "Allocations", "cold": "Cold start", "table": "Show the numbers as a table", "axis": "How far each loop falls below the raw client's best round", "rounds": "rounds, best to slowest", "best": "best round", "median": "median round", "noise": "spread of the rounds", "comparison": "Against a raw client", "code": "Cost of the code", "broker": "The broker", "plugin": "The {plugin} plugin", "pluginRaw": "Without the plugin", "pluginAdapter": "Plugin written by hand", "pluginFramework": "The crate's plugin", "pluginAxis": "How far each loop falls below the best round of the app without the plugin", "pluginAgainst": "({percent} over no plugin)", "pluginOverhead": "Over the plugin by hand", "pluginOverHand": "({percent} over by hand)"};

export const machine = {"cpu": "Processor", "architecture": "Architecture", "cpu_frequency": "Frequency", "cores": "Cores", "memory": "Memory", "memory_speed": "Memory speed", "os": "Operating system", "broker": "Broker", "rustc": "Compiler", "profile": "Build profile", "features": "Features", "rustflags": "RUSTFLAGS", "load": "Load average", "round_trip": "Round trip", "valgrind": "Valgrind"};

# Benchmarks

What `ruststream-rumqttc` costs on every message: throughput against the raw `rumqttc` client,
and instructions and allocations of its code.

## Against a raw client

The first chart runs each scenario three ways on a real MQTT server. `Raw client` drives
`rumqttc` directly. `Broker crate` drives this crate's subscription with a loop of its own and no
runtime. `RustStream` is the service a user writes, a `#[subscriber]` handler in an app. The first
gap is what this crate costs over the client it wraps, and the second is what the runtime adds on
top.

Each loop is a bar from its best round to its slowest, with a dot at the best round and a notch at
the median. The zero of the axis is the raw client's best round, and a dot right of zero is slower.
A hollow dot is a loop whose difference from the raw client is smaller than the spread of the
rounds. The table under the chart prints the best round with the median in parentheses.

The two scenarios are the two [qualities of service](../mqtt/qos.md) a service reads at. At QoS 0
the server sends each message and settles nothing. At QoS 1 the service
[acknowledges](../../components/settlement/index.md) each delivery with a PUBACK, as
[Path](../path.md) draws.

## Cost of the code

The second chart counts what one message costs on the service's thread: instructions under
callgrind and allocations under DHAT, in the steady state. Each scenario is a service on
[`MqttBroker`](../../components/broker/index.md) reading a QoS 1 topic on the same server. The
service runs on one thread, and the task that polls the `rumqttc` event loop runs there too, so the
count holds the framework, this crate and the client's own work on the socket, the PUBACK included.
The producer's thread is not counted. `Cold start` is what connecting, subscribing and taking the
first message cost once.

## Results

<Results entry={mqtt} labels={labels} details="/architecture/benchmarks/methodology/" />

## The machine

<Environment entry={mqtt} labels={machine} />

The numbers come from `docs/benchmarks/results.json` of this version, written by the benchmark
recipe. [Methodology](methodology.md) says how they were taken, and [Numbers](numbers.md) what
they allow you to conclude.

<MovedSections />
