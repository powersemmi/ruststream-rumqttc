set shell := ["bash", "-eu", "-o", "pipefail", "-c"]
set dotenv-load := false

export PATH := env("HOME") + "/.cargo/bin:" + env("HOME") + "/.local/bin:" + env("PATH")

default: check

check:
    cargo fmt --all -- --check
    # The benchmark package is left out of the all-features legs on purpose: it is built with the
    # feature set a service ships, and the framework's harness feature is a compile error in it.
    # Its own leg follows each of them.
    cargo clippy --workspace --exclude ruststream-rumqttc-bench --all-targets --all-features -- -D warnings
    cargo clippy -p ruststream-rumqttc-bench --all-targets -- -D warnings
    cargo check --workspace --exclude ruststream-rumqttc-bench --all-targets --all-features
    cargo check -p ruststream-rumqttc-bench --all-targets
    cargo check --workspace --no-default-features
    # CI denies rustdoc warnings, so a broken intra-doc link fails the build. Running it here is
    # what keeps that a local finding rather than a red pull request.
    RUSTDOCFLAGS="-D warnings" cargo doc --workspace --all-features --no-deps

test:
    cargo test --workspace --all-features

brokers-up: tls-certs
    docker compose -f docker-compose.test.yml up -d --wait

# The certificate chain the TLS listener uses, generated next to the stand rather than committed.
tls-certs:
    scripts/stand_tls_certs.sh "{{justfile_directory()}}/.stand-tls"

brokers-down:
    docker compose -f docker-compose.test.yml down -v

test-brokers: brokers-up
    #!/usr/bin/env bash
    set -euo pipefail
    trap 'just brokers-down' EXIT
    # This recipe starts the stand, so a gated test that skips itself here is a fault, not a
    # developer without a broker.
    MQTT_TEST_URL=mqtt://127.0.0.1:1883 \
    MQTT_TEST_AUTH_URL=mqtt://127.0.0.1:1884 \
    MQTT_TEST_TLS_URL=mqtts://127.0.0.1:8883 \
    MQTT_TEST_TLS_DIR={{justfile_directory()}}/.stand-tls \
    RUSTSTREAM_REQUIRE_LIVE=1 \
        cargo test --workspace --all-features -- --test-threads=1

# What this crate costs over the rumqttc client it wraps: two scenarios, one per quality of
# service, each run as a RustStream service and as a hand-written loop, against the stand the
# tests use. On demand only - it takes minutes and it wants the machine to itself. The page it
# feeds is docs/benchmarks.md.
bench *ARGS: brokers-up
    #!/usr/bin/env bash
    set -euo pipefail
    trap 'just brokers-down' EXIT
    mkdir -p target
    # RUSTFLAGS is cleared so the numbers are not tied to this machine's CPU: a binary built with
    # `-C target-cpu=native` cannot be reproduced anywhere else.
    RUSTFLAGS="" MQTT_TEST_URL=mqtt://127.0.0.1:1883 \
    RUSTSTREAM_BENCH_OUT="$PWD/target/bench-paired.json" \
        cargo bench -p ruststream-rumqttc-bench --bench paired {{ ARGS }}
    python3 scripts/bench_results.py target/bench-paired.json docs/benchmarks/results.json

# What a message costs on the service's thread, counted under valgrind: instructions through
# callgrind and allocations through DHAT, each scenario the service a user writes on the stand's
# mosquitto. The counts repeat within a fraction of a percent, so it needs the stand and not a quiet
# machine; it takes about a minute. The page it feeds is the code table of docs/benchmarks.md.
# RUSTFLAGS is cleared because valgrind aborts on the instructions a recent CPU advertises. Needs
# valgrind and the runner the benches pin: cargo install --locked gungraun-runner --version =0.19.4
# Extra arguments reach the runner: `just bench-code --save-baseline=main` records a baseline,
# `just bench-code --baseline=main` compares against it.
bench-code *ARGS: brokers-up
    #!/usr/bin/env bash
    set -euo pipefail
    trap 'just brokers-down' EXIT
    mkdir -p target
    RUSTFLAGS="" MQTT_TEST_URL=mqtt://127.0.0.1:1883 cargo bench -p ruststream-rumqttc-bench \
        --bench consume --bench reply --bench batch \
        -- --output-format=json {{ ARGS }} > target/bench-code.json
    python3 scripts/bench_results.py --code target/bench-code.json docs/benchmarks/results.json

fmt:
    cargo fmt --all

build:
    cargo build --workspace --release

security: deny zizmor

# Dependency-graph checks (advisories, licenses, duplicates, sources).
# Needs cargo-deny: cargo install cargo-deny --locked
deny:
    cargo deny check

zizmor:
    uvx zizmor .github/workflows

typo:
    uvx codespell

clean:
    cargo clean

ci: check test typo security
