/*
 * Renders the Benchmarks page from the document the crate publishes next to it,
 * `benchmarks/results.json`.
 *
 * The scenario table has three measured columns - the raw client, this crate's own consumer and
 * publisher, and the whole service - and the two differences that matter between them.
 *
 * The figures are fetched in the reader's browser rather than written into the page. A
 * re-measurement rewrites one JSON document, and a table copied into three translated pages
 * would be stale from the moment the next run finished. The pages therefore carry prose and no
 * figures at all, so there is nothing left to drift.
 *
 * Prose is never written here. Every label the page shows travels as JSON on the container, so
 * each translated page controls its own wording.
 *
 * A document that does not load, or that declares a schema this page does not render, leaves a
 * line saying so: a broken publish is visible instead of silently blank.
 *
 * No dependency and no build step. The script is a no-op on every page without the containers.
 */

(() => {
  "use strict";

  // The schema this page renders. A later revision may retype a field, and rendering it as if it
  // were this one would print wrong numbers instead of no numbers.
  // Schema 3 reports each loop as its best and worst round; a schema 1 document carried a
  // median with its extremes, and both render.
  const SCHEMAS = [1, 3];
  const TIMEOUT_MS = 8000;
  // Where the document sits when the page does not say. The English page is the one it sits
  // next to; a translated page carries the way back to it on the container.
  const DEFAULT_RESULTS = "results.json";

  // The machine and the build, in the order the schema documents the fields. Values are printed
  // as the document wrote them; the page adds no words of its own to them.
  const ENVIRONMENT = [
    ["machine", ["cpu", "architecture", "cpu_frequency", "cores", "memory", "memory_speed"]],
    ["os", ["os"]],
    ["broker", ["broker"]],
    ["roundTrip", ["round_trip"]],
    ["build", ["rustc", "profile", "features", "rustflags"]],
  ];

  const text = (tag, value) => {
    const node = document.createElement(tag);
    node.textContent = value;
    return node;
  };

  async function load(url) {
    const controller = new AbortController();
    const timer = setTimeout(() => controller.abort(), TIMEOUT_MS);
    try {
      const response = await fetch(url, { signal: controller.signal });
      return response.ok ? await response.json() : null;
    } catch {
      return null;
    } finally {
      clearTimeout(timer);
    }
  }

  const number = (value, lang) =>
    typeof value === "number" ? value.toLocaleString(lang, { maximumFractionDigits: 1 }) : "-";

  function side(measurement, unit, lang) {
    if (!measurement) {
      return "-";
    }
    if (typeof measurement.best === "number") {
      const best = number(measurement.best, lang) + " " + unit;
      if (typeof measurement.worst !== "number") {
        return best;
      }
      return best + " (" + number(measurement.worst, lang) + ")";
    }
    const median = number(measurement.median, lang) + " " + unit;
    if (typeof measurement.min !== "number" || typeof measurement.max !== "number") {
      return median;
    }
    return median + " (" + number(measurement.min, lang) + "-" + number(measurement.max, lang) + ")";
  }

  // A schema 3 loop is its best and worst round; a schema 1 loop was a median with its extremes.
  const figure = (measurement) =>
    typeof measurement?.best === "number" ? measurement.best : measurement?.median;
  const spread = (measurement) => {
    if (typeof measurement?.best === "number" && typeof measurement?.worst === "number") {
      return measurement.best - measurement.worst;
    }
    return typeof measurement?.min === "number" && typeof measurement?.max === "number"
      ? measurement.max - measurement.min
      : 0;
  };

  // The honesty rule of the methodology: a difference smaller than the run-to-run spread is a
  // verdict, never a percentage. The document decides it for the headline column, whose verdict it
  // publishes; for the adapter column the same rule is applied here, to the spreads it publishes.
  function indistinguishable(raw, side) {
    if (typeof figure(raw) !== "number" || typeof figure(side) !== "number") {
      return false;
    }
    return Math.abs(figure(raw) - figure(side)) < Math.max(spread(raw), spread(side));
  }

  function difference(percent, decided, labels) {
    if (typeof percent !== "number") {
      return "-";
    }
    return decided ? labels.indistinguishable : (percent >= 0 ? "+" : "") + percent + "%";
  }

  function overhead(scenario, labels) {
    let value = difference(
      scenario.overhead_percent,
      scenario.verdict === "indistinguishable",
      labels,
    );
    if (scenario.broker_bound) {
      value += " (" + labels.brokerBound + ")";
    }
    return value;
  }

  function scenarios(results, labels, lang) {
    const element = document.createElement("table");
    const head = element.createTHead().insertRow();
    for (const column of [
      labels.scenario,
      labels.raw,
      labels.adapter,
      labels.framework,
      labels.adapterOverhead,
      labels.overhead,
    ]) {
      head.appendChild(text("th", column));
    }
    const body = element.createTBody();
    for (const scenario of results.scenarios) {
      const row = body.insertRow();
      row.appendChild(text("td", scenario.name));
      row.appendChild(text("td", side(scenario.raw, scenario.unit, lang)));
      row.appendChild(text("td", side(scenario.adapter, scenario.unit, lang)));
      row.appendChild(text("td", side(scenario.framework, scenario.unit, lang)));
      row.appendChild(
        text(
          "td",
          difference(
            scenario.adapter_overhead_percent,
            indistinguishable(scenario.raw, scenario.adapter),
            labels,
          ),
        ),
      );
      row.appendChild(text("td", overhead(scenario, labels)));
    }
    return element;
  }

  function environment(results, labels) {
    const values = results.environment || {};
    const element = document.createElement("table");
    const body = element.createTBody();
    const row = (label, value) => {
      const line = body.insertRow();
      line.appendChild(text("th", label));
      line.appendChild(text("td", value));
    };
    for (const [label, fields] of ENVIRONMENT) {
      // `unknown` is how the document writes a field the machine does not publish, and a bare
      // "unknown" in a list of values reads as a value. Leaving it out says the same thing.
      const parts = fields.map((field) => values[field]).filter((v) => v && v !== "unknown");
      if (parts.length) {
        row(labels[label], parts.join(", "));
      }
    }
    row(
      labels.versions,
      results.crate + " " + results.crate_version + ", ruststream " + results.core_version,
    );
    row(labels.measured, results.measured_at);
    return element;
  }

  async function main() {
    const container = document.getElementById("benchmark-results");
    if (!container) {
      return;
    }
    const machine = document.getElementById("benchmark-environment");
    const lang = document.documentElement.lang || "en";
    const labels = JSON.parse(container.dataset.benchmarkLabels);
    const url = container.dataset.benchmarkResults || DEFAULT_RESULTS;
    for (const element of [container, machine]) {
      element?.replaceChildren(text("p", labels.loading));
    }

    const results = await load(url);
    const decline = (message) => {
      container.replaceChildren(text("p", message));
      machine?.replaceChildren();
    };
    if (!results) {
      decline(labels.unavailable.replace("{url}", new URL(url, location.href).href));
      return;
    }
    if (!SCHEMAS.includes(results.schema)) {
      decline(labels.unknownSchema.replace("{schema}", String(results.schema)));
      return;
    }
    if (!results.scenarios?.length) {
      decline(labels.unavailable.replace("{url}", new URL(url, location.href).href));
      return;
    }
    container.replaceChildren(scenarios(results, labels, lang));
    machine?.replaceChildren(environment(results, labels));
  }

  // Material swaps page content without a reload, so the tables are built on every navigation
  // rather than once per document.
  if (window.document$) {
    window.document$.subscribe(main);
  } else {
    document.addEventListener("DOMContentLoaded", main);
  }
})();
