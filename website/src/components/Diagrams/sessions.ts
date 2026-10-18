import type {Box, BoxKind, Lane, Link} from './flow';
import {lines, word, type Words} from './words';

type Side = 'service' | 'server';

interface Step {
  key: string;
  side: Side;
  kind: BoxKind;
  // Lines the box takes, when it holds more than two.
  rows?: number;
  href?: string;
}

/**
 * A persistent session from the first CONNECT to its resumption: the server keeps the session
 * past a lost connection, publishes the will, and sends what it kept once the same client id
 * connects again (MQTT 5, sections 3.1.2.4, 3.1.2.5, 3.2.2.1.1 and 4.1).
 */
export const STEPS: Step[] = [
  {key: 'connect', side: 'service', kind: 'step', rows: 3, href: '/components/broker/session/'},
  {key: 'connack', side: 'server', kind: 'broker'},
  {key: 'ping', side: 'service', kind: 'step', href: '/components/broker/reconnect/'},
  {key: 'lost', side: 'server', kind: 'bad'},
  {key: 'will', side: 'server', kind: 'bad', href: '/components/broker/last-will/'},
  {key: 'kept', side: 'server', kind: 'broker', rows: 3},
  {key: 'reconnect', side: 'service', kind: 'step', rows: 3, href: '/components/broker/reconnect/'},
  {key: 'present', side: 'server', kind: 'good'},
  {key: 'resend', side: 'server', kind: 'good'},
];

const LANE_X: Record<Side, number> = {service: 90, server: 270};
const STEP_W = 172;
const GAP = 14;
const TOP = 26;

/*
 * A session as a timeline: the service's column on the left, the server's on the right, each
 * step in the column of the side that takes it.
 */
export function sessions(words: Words): {boxes: Box[]; links: Link[]; lanes: Lane[]} {
  let y = TOP;
  const boxes: Box[] = STEPS.map((step) => {
    const h = 14 + 15 * (step.rows ?? 2);
    const box: Box = {
      id: step.key,
      x: LANE_X[step.side] - STEP_W / 2,
      y,
      w: STEP_W,
      h,
      kind: step.kind,
      lines: lines(words, step.key),
      href: step.href,
    };
    y += h + GAP;
    return box;
  });
  const links: Link[] = STEPS.slice(1).map((step, index) => ({
    from: STEPS[index]!.key,
    to: step.key,
  }));
  const lanes: Lane[] = (['service', 'server'] as const).map((side) => ({
    x: LANE_X[side],
    label: word(words, `${side}Lane`),
  }));
  return {boxes, links, lanes};
}
