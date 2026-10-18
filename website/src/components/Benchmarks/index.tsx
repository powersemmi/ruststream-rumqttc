import {useEffect, type CSSProperties, type ReactNode} from 'react';
import {useHistory, useLocation} from '@docusaurus/router';
import useBaseUrl from '@docusaurus/useBaseUrl';
import useDocusaurusContext from '@docusaurus/useDocusaurusContext';
import {movedSection} from './anchors';
import {environmentRows, type EnvironmentLabels} from './environment';
import {
  tabContent,
  type CodeBar,
  type CodePanel,
  type CodeRow,
  type ComparisonGroup,
  type ComparisonPanel,
  type Labels,
  type Lane,
  type Loaded,
  type Loop,
  type Tick,
} from './model';
import styles from './styles.module.css';

/*
 * The results of this crate's Benchmarks page: its comparison against the raw client and the
 * cost of its code, each a chart with the table it is drawn from, and the machine they were
 * taken on. The charts are the core's, so every RustStream site draws a result the same way.
 */

function useLang(): string {
  const {i18n} = useDocusaurusContext();
  return i18n.localeConfigs[i18n.currentLocale]?.htmlLang ?? i18n.currentLocale;
}

// Positions arrive as percentages of the plot, so every mark is laid out by CSS alone.
const at = (left: number, right?: number): CSSProperties =>
  right === undefined ? {left: `${left}%`} : {left: `${left}%`, width: `${right - left}%`};

const LOOP_CLASS: Record<Loop, string | undefined> = {
  raw: styles.raw,
  adapter: styles.adapter,
  framework: styles.framework,
};

const LOOPS: Loop[] = ['raw', 'adapter', 'framework'];

function Grid({ticks, zero}: {ticks: Tick[]; zero?: number}): ReactNode {
  return (
    <>
      {ticks.map((tick) => (
        <span key={tick.value} className={styles.gridline} style={at(tick.position)} />
      ))}
      {zero !== undefined && <span className={styles.zero} style={at(zero)} />}
    </>
  );
}

function TickLabels({ticks}: {ticks: Tick[]}): ReactNode {
  return (
    <div className={styles.ticks} aria-hidden="true">
      {ticks.map((tick) => (
        <span key={tick.value} className={styles.tick} style={at(tick.position)}>
          {tick.label}
        </span>
      ))}
    </div>
  );
}

function Swatch({loop}: {loop: Loop}): ReactNode {
  return <span className={`${styles.swatch} ${LOOP_CLASS[loop]}`} aria-hidden="true" />;
}

function LaneMarks({lane}: {lane: Lane}): ReactNode {
  return (
    <div className={`${styles.lane} ${LOOP_CLASS[lane.loop]}`}>
      <span className={styles.range} style={at(lane.from, lane.to)} />
      {lane.median !== undefined && <span className={styles.median} style={at(lane.median)} />}
      <span className={`${styles.dot} ${lane.inside ? styles.hollow : ''}`} style={at(lane.best)} />
    </div>
  );
}

function ComparisonLegend({labels}: {labels: Labels}): ReactNode {
  return (
    <ul className={styles.legend}>
      {LOOPS.map((loop) => (
        <li key={loop}>
          <Swatch loop={loop} />
          {labels[loop]}
        </li>
      ))}
      <li>
        <span className={`${styles.keyRange} ${styles.adapter}`} aria-hidden="true" />
        {labels.rounds}
      </li>
      <li>
        <span className={`${styles.keyDot} ${styles.adapter}`} aria-hidden="true" />
        {labels.best}
      </li>
      <li>
        <span className={`${styles.keyMedian} ${styles.adapter}`} aria-hidden="true" />
        {labels.median}
      </li>
      <li>
        <span className={styles.keyBand} aria-hidden="true" />
        {labels.noise}
      </li>
      <li>
        <span
          className={`${styles.keyDot} ${styles.hollow} ${styles.adapter}`}
          aria-hidden="true"
        />
        {labels.indistinguishable}
      </li>
    </ul>
  );
}

function ComparisonChart({
  panel,
  labels,
  provenance,
  details,
}: {
  panel: ComparisonPanel;
  labels: Labels;
  provenance: boolean;
  details: string;
}): ReactNode {
  return (
    <section className={styles.panel}>
      {provenance && (
        <p className={styles.panelHead}>
          {panel.provenance}. <a href={details}>{labels.details}</a>
        </p>
      )}
      <div className={`${styles.row} ${styles.axisRow}`}>
        <TickLabels ticks={panel.ticks} />
      </div>
      {panel.rows.map((row) => (
        <div key={row.scenario} className={styles.row} tabIndex={0}>
          <span className={styles.name}>{row.scenario}</span>
          <div className={styles.plot} aria-hidden="true">
            <Grid ticks={panel.ticks} zero={panel.zero} />
            {row.lanes
              .filter((lane) => lane.loop === 'framework' && lane.band)
              .map((lane) => (
                <span
                  key="band"
                  className={styles.band}
                  style={at(lane.band!.from, lane.band!.to)}
                />
              ))}
            {row.lanes.map((lane) => (
              <LaneMarks key={lane.loop} lane={lane} />
            ))}
          </div>
          <ul className={styles.verdicts}>
            {row.lanes
              .filter((lane) => lane.verdict !== undefined)
              .map((lane) => (
                <li key={lane.loop}>
                  <Swatch loop={lane.loop} />
                  {lane.verdict}
                </li>
              ))}
          </ul>
          <dl className={styles.tooltip} aria-hidden="true">
            {row.lanes.map((lane) => (
              <div key={lane.loop}>
                <dt>
                  <Swatch loop={lane.loop} />
                  {labels[lane.loop]}
                </dt>
                <dd>
                  <strong>{lane.value}</strong>
                </dd>
              </div>
            ))}
          </dl>
        </div>
      ))}
    </section>
  );
}

/**
 * Throughput against the raw loop, as the crate measures it: one chart per group, so a plugin
 * whose overhead is on another scale gets an axis of its own. The run is named once, above the
 * first chart.
 */
function Comparison({
  group: {title, plugin, labels, panel, rows},
  provenance,
  details,
}: {
  group: ComparisonGroup;
  provenance: boolean;
  details: string;
}): ReactNode {
  return (
    <figure className={styles.chart}>
      <figcaption className={styles.caption}>
        <strong>{title}</strong>
        <span>{labels.axis}</span>
      </figcaption>
      <ComparisonLegend labels={labels} />
      <ComparisonChart
        panel={panel}
        labels={labels}
        provenance={provenance}
        details={details}
      />
      <details className={styles.table}>
        <summary>{labels.table}</summary>
        <table>
          <thead>
            <tr>
              <th>{labels.scenario}</th>
              <th>{labels.raw}</th>
              <th>{labels.adapter}</th>
              <th>{labels.framework}</th>
              <th>{labels.overhead}</th>
              {plugin !== null && <th>{labels.pluginOverhead}</th>}
            </tr>
          </thead>
          <tbody>
            {rows.map((row) => (
              <tr key={row.scenario}>
                <td>{row.scenario}</td>
                <td>{row.raw}</td>
                <td>{row.adapter}</td>
                <td>{row.framework}</td>
                <td>{row.overhead}</td>
                {plugin !== null && <td>{row.plugin}</td>}
              </tr>
            ))}
          </tbody>
        </table>
      </details>
    </figure>
  );
}

function Bar({bar}: {bar: CodeBar}): ReactNode {
  return (
    <div className={styles.measure}>
      <div className={styles.track} aria-hidden="true">
        <span className={styles.bar} style={{width: `${bar.width}%`}} />
      </div>
      <span className={styles.value}>{bar.value}</span>
    </div>
  );
}

function CodeChart({panel, labels}: {panel: CodePanel; labels: Labels}): ReactNode {
  return (
    <section className={styles.codePanel}>
      <p className={styles.panelHead}>{panel.machine}</p>
      <div className={`${styles.codeRow} ${styles.axisRow}`}>
        {[
          [labels.instructions, panel.instructions] as const,
          [labels.allocations, panel.allocations] as const,
        ].map(([name, ticks]) => (
          <div key={name} className={styles.measure}>
            <div>
              <span className={styles.measureName}>{name}</span>
              <TickLabels ticks={ticks} />
            </div>
          </div>
        ))}
      </div>
      {panel.rows.map((row) => (
        <div key={row.scenario} className={styles.codeRow} tabIndex={0}>
          <span className={styles.name}>{row.scenario}</span>
          <Bar bar={row.instructions} />
          <Bar bar={row.allocations} />
          <dl className={styles.tooltip} aria-hidden="true">
            {[
              [labels.instructions, row.instructions.value],
              [labels.allocations, row.allocations.value],
              [labels.cold, row.cold],
            ].map(([name, value]) => (
              <div key={name}>
                <dt>{name}</dt>
                <dd>
                  <strong>{value}</strong>
                </dd>
              </div>
            ))}
          </dl>
        </div>
      ))}
    </section>
  );
}

/** Instructions and allocations per message, as the crate measures them. */
function Code({
  panel,
  rows,
  labels,
}: {
  panel: CodePanel;
  rows: CodeRow[];
  labels: Labels;
}): ReactNode {
  return (
    <figure className={styles.chart}>
      <figcaption className={styles.caption}>
        <strong>{labels.code}</strong>
      </figcaption>
      <CodeChart panel={panel} labels={labels} />
      <details className={styles.table}>
        <summary>{labels.table}</summary>
        <table>
          <thead>
            <tr>
              <th>{labels.scenario}</th>
              <th>{labels.instructions}</th>
              <th>{labels.allocations}</th>
              <th>{labels.cold}</th>
            </tr>
          </thead>
          <tbody>
            {rows.map((row) => (
              <tr key={row.scenario}>
                <td>{row.scenario}</td>
                <td>{row.instructions}</td>
                <td>{row.allocations}</td>
                <td>{row.cold}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </details>
    </figure>
  );
}

function CrateResults({
  entry,
  labels,
  lang,
  details,
}: {
  entry: Loaded;
  labels: Labels;
  lang: string;
  details: string;
}): ReactNode {
  const {comparison, code, pending} = tabContent(entry, lang, labels);
  return (
    <>
      {comparison.map((group, index) => (
        <Comparison
          key={group.title}
          group={group}
          provenance={index === 0}
          details={details}
        />
      ))}
      {code && <Code {...code} labels={labels} />}
      {pending && <p>{pending}</p>}
    </>
  );
}

/**
 * A crate's comparison and cost charts, from the results document its adapter reads. `details`
 * is the page that explains how the numbers were taken.
 */
export function Results({
  entry,
  labels,
  details,
}: {
  entry: Loaded;
  labels: Labels;
  details: string;
}): ReactNode {
  const lang = useLang();
  return (
    <div className={styles.results}>
      <CrateResults entry={entry} labels={labels} lang={lang} details={useBaseUrl(details)} />
    </div>
  );
}

/** The machine and the build the results were taken on, every field as the document has it. */
export function Environment({entry, labels}: {entry: Loaded; labels: EnvironmentLabels}): ReactNode {
  const rows = environmentRows(entry.results?.environment ?? {}, labels);
  return (
    <table>
      <tbody>
        {rows.map((row) => (
          <tr key={row.field}>
            <th scope="row">{row.label}</th>
            <td>{row.value}</td>
          </tr>
        ))}
      </tbody>
    </table>
  );
}

/** Sends a link to a section that left the Benchmarks page on to the page that took it over. */
export function MovedSections(): null {
  const history = useHistory();
  const {pathname, hash} = useLocation();
  useEffect(() => {
    const target = movedSection(hash);
    if (target) {
      history.replace(`${pathname.replace(/\/?$/, '/')}${target}`);
    }
  }, [history, pathname, hash]);
  return null;
}
