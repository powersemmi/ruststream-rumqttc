import {describe, expect, it} from 'vitest';
import {FLOWS, qos} from './qos';
import type {Words} from './words';

// Every word is its own key, so the test reads the layout and never a page's wording.
const words = new Proxy({}, {get: (_, key) => String(key)}) as Words;

const packets = (flow: (typeof FLOWS)[number]): string[] =>
  flow.steps.flatMap((step) => ('packet' in step ? [`${step.side} ${step.packet}`] : []));

describe('the packets of a delivery at each quality of service', () => {
  it('are the exchanges MQTT 5 defines, the server sending and the service answering', () => {
    expect(FLOWS.map((flow) => packets(flow))).toEqual([
      ['server PUBLISH'],
      ['server PUBLISH', 'service PUBACK'],
      ['server PUBLISH', 'service PUBREC', 'server PUBREL', 'service PUBCOMP'],
    ]);
  });

  it('runs the handler before the service answers anything', () => {
    for (const flow of FLOWS) {
      const handler = flow.steps.findIndex((step) => step.key.startsWith('handler'));
      expect(handler, flow.key).toBe(1);
      expect(flow.steps[handler]!.side).toBe('service');
    }
  });
});

describe('the qos diagram', () => {
  const {boxes, links, lanes} = qos(words);

  it('draws each step in the column of the side that takes it', () => {
    const column = Object.fromEntries(lanes.map((lane) => [lane.label, lane.x]));
    for (const step of FLOWS.flatMap((flow) => flow.steps)) {
      const box = boxes.find((candidate) => candidate.id === step.key)!;
      expect(box.x + box.w / 2, step.key).toBe(column[`${step.side}Lane`]);
    }
  });

  it('links each step to the next one of its flow, and never across flows', () => {
    const expected = FLOWS.flatMap((flow) =>
      flow.steps.slice(1).map((step, index) => ({from: flow.steps[index]!.key, to: step.key})),
    );
    expect(links.map(({from, to}) => ({from, to}))).toEqual(expected);
  });
});
