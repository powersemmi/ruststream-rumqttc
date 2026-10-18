import type {Box, Link} from './flow';
import {matches} from './topics';
import {lines, word, type Words} from './words';

export interface Packet {
  id: string;
  topic: string;
  identifier: number;
}

const TOPIC = 'devices/dev42/telemetry';

/**
 * The filters one connection holds when the drawing's packets arrive. Both match the topic, so
 * each carries a subscription identifier; the first is opened by two subscriptions, which take
 * its deliveries in turn.
 */
export const WIRES = [
  {id: 'wire1', filter: 'devices/+/telemetry', identifier: 17, members: ['first', 'second']},
  {id: 'wire2', filter: 'devices/#', identifier: 42, members: ['only']},
] as const;

/** The server sends one PUBLISH per matching subscription, each naming its identifier. */
export const PACKETS: Packet[] = WIRES.map((wire) => ({
  id: `packet${wire.identifier}`,
  topic: TOPIC,
  identifier: wire.identifier,
}));

/** The filter a packet belongs to: the one whose identifier it names and whose filter matches. */
export function route(packet: Packet): (typeof WIRES)[number] | undefined {
  return WIRES.find(
    (wire) => wire.identifier === packet.identifier && matches(wire.filter, packet.topic),
  );
}

const COLUMN = 168;
const COLUMNS = [0, 192] as const;
const MEMBER_W = 80;

/*
 * One connection demultiplexing the packets the server sends it: each packet names the
 * subscription identifier of the filter it was sent for, the registry hands it to that filter,
 * and the filter's subscriptions take its deliveries in turn.
 */
export function demultiplexing(words: Words): {boxes: Box[]; links: Link[]} {
  const boxes: Box[] = [
    {
      id: 'registry',
      x: 0,
      y: 86,
      w: 360,
      h: 170,
      kind: 'group',
      lines: lines(words, 'registry'),
      href: '/components/subscriptions/overlaps/',
    },
  ];
  const links: Link[] = [];
  PACKETS.forEach((packet, column) => {
    const x = COLUMNS[column]!;
    const wire = route(packet)!;
    boxes.push(
      {
        id: packet.id,
        x,
        y: 0,
        w: COLUMN,
        h: 59,
        kind: 'broker',
        code: true,
        lines: ['PUBLISH', packet.topic, `${word(words, 'identifier')} ${packet.identifier}`],
      },
      {
        id: wire.id,
        x,
        y: 112,
        w: COLUMN,
        h: 59,
        kind: 'core',
        code: true,
        lines: [wire.filter, `${word(words, 'identifier')} ${wire.identifier}`],
      },
    );
    links.push({from: packet.id, to: wire.id});
    // A filter's subscriptions share its column; the one whose turn it is takes the packet.
    const width = wire.members.length === 1 ? COLUMN : MEMBER_W;
    wire.members.forEach((member, index) => {
      const memberX = x + index * (COLUMN - MEMBER_W);
      boxes.push({
        id: member,
        x: memberX,
        y: 200,
        w: width,
        h: 44,
        kind: index === 0 ? 'good' : 'step',
        lines: lines(words, member),
      });
      links.push({
        from: wire.id,
        to: member,
        exitAt: (memberX - x + width / 2) / COLUMN,
        ...(index > 0 ? {dashed: true, label: word(words, 'next')} : {}),
      });
    });
  });
  boxes.push({
    id: 'held',
    x: 0,
    y: 276,
    w: 360,
    h: 44,
    kind: 'note',
    lines: lines(words, 'held'),
  });
  return {boxes, links};
}
