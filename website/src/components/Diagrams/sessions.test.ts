import {describe, expect, it} from 'vitest';
import {STEPS, sessions} from './sessions';
import type {Words} from './words';

// Every word is its own key, so the test reads the layout and never a page's wording.
const words = new Proxy({}, {get: (_, key) => String(key)}) as Words;

const order = (key: string): number => STEPS.findIndex((step) => step.key === key);

describe('the life of a persistent session', () => {
  it('publishes the will only once the connection is lost', () => {
    expect(order('lost')).toBeGreaterThan(0);
    expect(order('will')).toBe(order('lost') + 1);
  });

  it('sends what it kept only after a CONNACK that reports the session present', () => {
    expect(order('present')).toBeGreaterThan(order('reconnect'));
    expect(order('resend')).toBe(order('present') + 1);
  });

  it('leaves each CONNECT from the service and each answer from the server', () => {
    const side = Object.fromEntries(STEPS.map((step) => [step.key, step.side]));
    expect([side.connect, side.ping, side.reconnect]).toEqual(['service', 'service', 'service']);
    expect([side.connack, side.lost, side.will, side.kept, side.present, side.resend]).toEqual([
      'server',
      'server',
      'server',
      'server',
      'server',
      'server',
    ]);
  });
});

describe('the sessions diagram', () => {
  const {boxes, links, lanes} = sessions(words);

  it('draws each step in its side column, one under another, linked in order', () => {
    const column = Object.fromEntries(lanes.map((lane) => [lane.label, lane.x]));
    let previous = -1;
    for (const step of STEPS) {
      const box = boxes.find((candidate) => candidate.id === step.key)!;
      expect(box.x + box.w / 2, step.key).toBe(column[`${step.side}Lane`]);
      expect(box.y, step.key).toBeGreaterThan(previous);
      previous = box.y;
    }
    expect(links.map(({from, to}) => [from, to])).toEqual(
      STEPS.slice(1).map((step, index) => [STEPS[index]!.key, step.key]),
    );
  });
});
