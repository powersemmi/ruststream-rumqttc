import type {Box, Lane, Link} from './flow';
import {lines, word, type Words} from './words';

type Side = 'server' | 'service';

interface Step {
  key: string;
  side: Side;
  // The packet the step sends, as the protocol names it; a step without one is the handler.
  packet?: string;
}

/**
 * One delivery at each quality of service: the PUBLISH the server sends, the handler, and the
 * packets the service answers with (MQTT 5, sections 4.3.1 to 4.3.3). With manual
 * acknowledgements the client sends PUBACK or PUBREC only when the delivery is acknowledged, and
 * completes the PUBREL and PUBCOMP exchange of QoS 2 itself.
 */
export const FLOWS: {key: string; href: string; steps: Step[]}[] = [
  {
    key: 'qos0',
    href: '/components/subscriptions/qos/',
    steps: [
      {key: 'publish0', side: 'server', packet: 'PUBLISH'},
      {key: 'handler0', side: 'service'},
    ],
  },
  {
    key: 'qos1',
    href: '/components/settlement/',
    steps: [
      {key: 'publish1', side: 'server', packet: 'PUBLISH'},
      {key: 'handler1', side: 'service'},
      {key: 'puback', side: 'service', packet: 'PUBACK'},
    ],
  },
  {
    key: 'qos2',
    href: '/components/settlement/',
    steps: [
      {key: 'publish2', side: 'server', packet: 'PUBLISH'},
      {key: 'handler2', side: 'service'},
      {key: 'pubrec', side: 'service', packet: 'PUBREC'},
      {key: 'pubrel', side: 'server', packet: 'PUBREL'},
      {key: 'pubcomp', side: 'service', packet: 'PUBCOMP'},
    ],
  },
];

const LANE_X: Record<Side, number> = {server: 90, service: 270};
const STEP_W = 156;
const STEP_H = 36;
const GAP = 12;
// Room for a frame's name above its first step.
const HEAD = 26;
const TOP = 26;

/*
 * The three qualities of service as a timeline: the server's column on the left, the service's
 * on the right, one frame per level, and each packet in the column of the side that sends it.
 */
export function qos(words: Words): {boxes: Box[]; links: Link[]; lanes: Lane[]} {
  const boxes: Box[] = [];
  const links: Link[] = [];
  let y = TOP;
  for (const flow of FLOWS) {
    const height = HEAD + flow.steps.length * (STEP_H + GAP) - GAP + 12;
    boxes.push({
      id: flow.key,
      x: 0,
      y,
      w: 360,
      h: height,
      kind: 'group',
      lines: lines(words, flow.key),
      href: flow.href,
    });
    flow.steps.forEach((step, index) => {
      // A packet box names the packet, then what the page adds to it, if anything.
      const extra = word(words, step.key);
      boxes.push({
        id: step.key,
        x: LANE_X[step.side] - STEP_W / 2,
        y: y + HEAD + index * (STEP_H + GAP),
        w: STEP_W,
        h: STEP_H,
        kind: step.packet ? (step.side === 'server' ? 'broker' : 'step') : 'service',
        lines: step.packet
          ? [extra ? `${step.packet} ${extra}` : step.packet]
          : lines(words, step.key),
      });
      if (index > 0) {
        links.push({from: flow.steps[index - 1]!.key, to: step.key});
      }
    });
    y += height + 12;
  }
  const lanes: Lane[] = (['server', 'service'] as const).map((side) => ({
    x: LANE_X[side],
    label: word(words, `${side}Lane`),
  }));
  return {boxes, links, lanes};
}
