import type {Box, Lane, Link} from './flow';
import {lines, word, type Words} from './words';

/**
 * What a QoS 1 or QoS 2 delivery sends for each outcome of a handler, as the protocol writes it,
 * or `null` where nothing is sent. MQTT has one acknowledgement and no negative one: PUBACK at
 * QoS 1, PUBREC at QoS 2.
 */
export const OUTCOMES = [
  {id: 'ack', wire: 'PUBACK / PUBREC'},
  {id: 'drop', wire: 'PUBACK / PUBREC'},
  {id: 'retry', wire: null},
  {id: 'retryCapped', wire: 'PUBACK / PUBREC'},
  {id: 'retryAfter', wire: 'PUBACK / PUBREC'},
  {id: 'unsettled', wire: null},
] as const;

const COLUMN = 112;
const COLUMNS = [0, 124, 248] as const;
const ROW = 58;
const GAP = 12;
const TOP = 28;

const HREFS: Record<string, string> = {
  retry: '/components/settlement/retry/',
  retryCapped: '/components/settlement/retries/',
  retryAfter: '/components/settlement/retry-after/',
};

/*
 * The settlement table drawn as a timeline of three columns: what the handler returns, what the
 * delivery sends to the server, and what follows for the message.
 */
export function settlement(words: Words): {boxes: Box[]; links: Link[]; lanes: Lane[]} {
  const cell = (id: string, column: number, row: number, fields: Partial<Box>): Box => ({
    id,
    x: COLUMNS[column]!,
    y: TOP + row * (ROW + GAP),
    w: COLUMN,
    h: ROW,
    kind: 'step',
    href: HREFS[id],
    ...fields,
    // A cell that brings its own lines, the protocol's, takes no words from the page.
    lines: fields.lines ?? lines(words, id),
  });
  const boxes: Box[] = OUTCOMES.flatMap((outcome, row) => [
    cell(outcome.id, 0, row, {kind: 'service', code: outcome.id !== 'unsettled'}),
    cell(`${outcome.id}Wire`, 1, row, {
      kind: 'broker',
      code: outcome.wire !== null,
      // The two packets go one to a line, where they fit.
      lines: outcome.wire === null ? lines(words, 'nothing') : outcome.wire.split(' / '),
    }),
    cell(`${outcome.id}Effect`, 2, row, {
      kind: outcome.id === 'ack' ? 'good' : outcome.id === 'drop' ? 'bad' : 'core',
    }),
  ]);
  boxes.push({
    id: 'qos0',
    x: 0,
    y: TOP + OUTCOMES.length * (ROW + GAP) + 8,
    w: 360,
    h: 44,
    kind: 'note',
    lines: lines(words, 'qos0'),
    href: '/components/subscriptions/qos/',
  });
  const links: Link[] = OUTCOMES.flatMap((outcome) => [
    {from: outcome.id, to: `${outcome.id}Wire`},
    {from: `${outcome.id}Wire`, to: `${outcome.id}Effect`},
  ]);
  const lanes: Lane[] = (['outcomeLane', 'wireLane', 'effectLane'] as const).map(
    (key, column) => ({x: COLUMNS[column]! + COLUMN / 2, label: word(words, key)}),
  );
  return {boxes, links, lanes};
}
