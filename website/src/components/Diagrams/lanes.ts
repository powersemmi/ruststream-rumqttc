import type {Box, Link} from './flow';
import {lines, word, type Words} from './words';

const LANE_X = [12, 188] as const;

/*
 * One subscription's dispatch: its stream hands the deliveries over one at a time, and
 * `workers(n)` spreads them over lanes in turn, because an MQTT delivery carries no key. Across
 * processes, a share group divides the messages instead.
 */
export function lanes(words: Words): {boxes: Box[]; links: Link[]} {
  const boxes: Box[] = [
    {
      id: 'subscription',
      x: 12,
      y: 0,
      w: 336,
      h: 44,
      kind: 'broker',
      lines: lines(words, 'subscription'),
    },
    {id: 'stream', x: 12, y: 72, w: 336, h: 44, kind: 'broker', lines: lines(words, 'stream')},
    {id: 'pool', x: 12, y: 144, w: 336, h: 44, kind: 'core', lines: lines(words, 'pool')},
    ...LANE_X.map(
      (x, lane): Box => ({
        id: `lane${lane}`,
        x,
        y: 220,
        w: 160,
        h: 58,
        kind: 'service',
        lines: lines(words, `lane${lane}`),
      }),
    ),
    {id: 'noKey', x: 0, y: 298, w: 360, h: 44, kind: 'note', lines: lines(words, 'noKey')},
    {
      id: 'scale',
      x: 0,
      y: 350,
      w: 360,
      h: 44,
      kind: 'note',
      lines: lines(words, 'scale'),
      href: '/components/subscriptions/shared/',
    },
  ];
  const links: Link[] = [
    {from: 'subscription', to: 'stream', label: word(words, 'delivered')},
    {from: 'stream', to: 'pool', label: word(words, 'oneAtATime')},
    ...LANE_X.map((x, lane) => ({from: 'pool', to: `lane${lane}`, exitAt: (x + 80 - 12) / 336})),
  ];
  return {boxes, links};
}
