import {describe, expect, it} from 'vitest';
import {OUTCOMES, settlement} from './settlement';
import type {Words} from './words';

// Every word is its own key, so the test reads the layout and never a page's wording.
const words = new Proxy({}, {get: (_, key) => String(key)}) as Words;

describe('what a QoS 1 or 2 delivery sends for each outcome', () => {
  it('is the acknowledgement rumqttc writes for the call the crate and the runtime make', () => {
    // `ack` and `nack(false)` call `AsyncClient::ack`, which writes PUBACK at QoS 1 and PUBREC at
    // QoS 2. `nack(true)` reports `Unsupported` and writes nothing; a copy the runtime publishes
    // drops the original with `nack(false)` first.
    expect(Object.fromEntries(OUTCOMES.map((outcome) => [outcome.id, outcome.wire]))).toEqual({
      ack: 'PUBACK / PUBREC',
      drop: 'PUBACK / PUBREC',
      retry: null,
      retryCapped: 'PUBACK / PUBREC',
      retryAfter: 'PUBACK / PUBREC',
      unsettled: null,
    });
  });

  it('draws each outcome to its wire form and on to what follows', () => {
    const {links} = settlement(words);
    for (const outcome of OUTCOMES) {
      const out = links.filter((link) => link.from === outcome.id).map((link) => link.to);
      expect(out).toEqual([`${outcome.id}Wire`]);
      const effect = links.filter((link) => link.from === `${outcome.id}Wire`).map((l) => l.to);
      expect(effect).toEqual([`${outcome.id}Effect`]);
    }
  });
});
