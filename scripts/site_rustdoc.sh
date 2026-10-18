#!/usr/bin/env bash
# Builds the rustdoc the site's API links resolve against, into target/doc: this crate with the
# features docs.rs renders it with, and the core release the lock file pins with every feature,
# as docs.rs renders the core. A page may link items of either crate.
set -euo pipefail
cd "$(dirname "$0")/.."
features="$(cargo metadata --locked --format-version 1 | python3 -c '
import json, sys
packages = {p["name"]: p for p in json.load(sys.stdin)["packages"]}
mqtt = packages["ruststream-rumqttc"]
docs_rs = mqtt["metadata"]["docs"]["rs"]
# docs.rs renders this crate with every feature on.
crate = (
    [name for name in mqtt["features"] if name != "default"]
    if docs_rs.get("all-features")
    else docs_rs.get("features", [])
)
core = [f"ruststream/{name}" for name in packages["ruststream"]["features"] if name != "default"]
print(",".join(crate + core))
')"
cargo doc --locked --no-deps -p ruststream-rumqttc -p ruststream --features "$features"
