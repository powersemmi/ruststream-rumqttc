import {describe, expect, it} from 'vitest';
import {PACKETS, WIRES, demultiplexing, route} from './demultiplexing';
import {matches} from './topics';
import type {Words} from './words';

// Every word is its own key, so the test reads the layout and never a page's wording.
const words = new Proxy({}, {get: (_, key) => String(key)}) as Words;

describe('a PUBLISH packet on a connection with overlapping filters', () => {
  it('matches every filter on the connection, so the topic alone cannot tell them apart', () => {
    for (const packet of PACKETS) {
      for (const wire of WIRES) {
        expect(matches(wire.filter, packet.topic), `${packet.id} ${wire.id}`).toBe(true);
      }
    }
  });

  it('goes to the one filter whose subscription identifier it names', () => {
    expect(PACKETS.map((packet) => route(packet)?.id)).toEqual(['wire1', 'wire2']);
    expect(route({id: 'stray', topic: PACKETS[0]!.topic, identifier: 7})).toBeUndefined();
  });
});

describe('the demultiplexing diagram', () => {
  const {links} = demultiplexing(words);
  const to = (id: string): string[] =>
    links.filter((link) => link.from === id && !link.dashed).map((link) => link.to);

  it('hands each packet to its filter, and to the member whose turn it is', () => {
    for (const packet of PACKETS) {
      const wire = route(packet)!;
      expect(to(packet.id)).toEqual([wire.id]);
      expect(to(wire.id)).toEqual([wire.members[0]]);
    }
  });
});
