import {describe, expect, it} from 'vitest';
import {SHARE_GROUP, SUBSCRIPTIONS, TOPIC, matches, topics} from './topics';
import type {Words} from './words';

// Every word is its own key, so the test reads the layout and never a page's wording.
const words = new Proxy({}, {get: (_, key) => String(key)}) as Words;

describe('a topic filter against a published topic', () => {
  it('matches an exact topic level for level', () => {
    expect(matches('devices/dev42/telemetry', 'devices/dev42/telemetry')).toBe(true);
    expect(matches('devices/dev42/telemetry', 'devices/dev7/telemetry')).toBe(false);
    expect(matches('devices/dev42', 'devices/dev42/telemetry')).toBe(false);
  });

  it('takes + for exactly one level, an empty one included', () => {
    expect(matches('devices/+/telemetry', 'devices/dev42/telemetry')).toBe(true);
    expect(matches('devices/+/telemetry', 'devices//telemetry')).toBe(true);
    expect(matches('devices/+', 'devices/dev42/telemetry')).toBe(false);
    expect(matches('devices/+/telemetry', 'devices/telemetry')).toBe(false);
  });

  it('takes # for the rest of the topic, the parent level included', () => {
    expect(matches('devices/#', 'devices/dev42/telemetry')).toBe(true);
    expect(matches('devices/#', 'devices')).toBe(true);
    expect(matches('#', 'devices/dev42/telemetry')).toBe(true);
    expect(matches('devices/#', 'alerts')).toBe(false);
  });

  it('keeps a topic that starts with $ from a filter that starts with a wildcard', () => {
    expect(matches('#', '$SYS/broker/uptime')).toBe(false);
    expect(matches('+/broker/uptime', '$SYS/broker/uptime')).toBe(false);
    expect(matches('$SYS/#', '$SYS/broker/uptime')).toBe(true);
  });
});

describe('the topics diagram', () => {
  const {boxes, links} = topics(words);
  const reached = links.filter((link) => link.from === 'server').map((link) => link.to);

  it('delivers to every subscription whose filter matches, and to no other', () => {
    const matching = SUBSCRIPTIONS.filter((sub) => matches(sub.filter, TOPIC)).map(
      (sub) => sub.id,
    );
    expect(matching.length).toBeGreaterThan(0);
    expect(matching.length).toBeLessThan(SUBSCRIPTIONS.length);
    for (const sub of SUBSCRIPTIONS) {
      expect(reached.includes(sub.id), sub.id).toBe(matching.includes(sub.id));
    }
  });

  it('hands a matching share group the message once, to one member', () => {
    expect(matches(SHARE_GROUP.filter, TOPIC)).toBe(true);
    const members = SHARE_GROUP.members.filter((member) => reached.includes(member));
    expect(members).toHaveLength(1);
  });

  it('sends the retained message of the topic to a subscription that joins later', () => {
    expect(links).toContainEqual(expect.objectContaining({from: 'retained', to: 'later'}));
  });

  it('writes each topic and filter as the protocol spells it, whatever the language', () => {
    const lines = boxes.flatMap((box) => box.lines);
    for (const sub of SUBSCRIPTIONS) {
      expect(lines).toContain(sub.filter);
    }
    expect(lines).toContain(TOPIC);
    expect(lines).toContain(SHARE_GROUP.filter);
  });
});
