import type {Box, Link} from './flow';
import {lines, word, type Words} from './words';

// The topic the drawing publishes, and the subscriptions the server holds when it arrives.
// Topics and filters are protocol text, written as the wire spells them in every language.
export const TOPIC = 'devices/dev42/telemetry';

export const SUBSCRIPTIONS = [
  {id: 'exact', filter: 'devices/dev42/telemetry'},
  {id: 'one', filter: 'devices/+/telemetry'},
  {id: 'rest', filter: 'devices/#'},
  {id: 'other', filter: 'devices/+/state'},
] as const;

export const SHARE_GROUP = {
  name: 'workers',
  filter: 'devices/+/telemetry',
  members: ['member1', 'member2'],
} as const;

/**
 * Whether a topic filter takes a published topic, level by level: `+` stands for exactly one
 * level, and `#`, last, for the rest of the topic, the parent level included. A topic that starts
 * with `$` is never taken by a filter that starts with a wildcard (MQTT 5, section 4.7.2).
 */
export function matches(filter: string, topic: string): boolean {
  const want = filter.split('/');
  const have = topic.split('/');
  if (topic.startsWith('$') && (want[0] === '+' || want[0] === '#')) {
    return false;
  }
  for (const [index, level] of want.entries()) {
    if (level === '#') {
      return index === want.length - 1;
    }
    if (index >= have.length || (level !== '+' && level !== have[index])) {
      return false;
    }
  }
  return want.length === have.length;
}

const SERVER_W = 120;
const SUB_X = 150;
const SUB_W = 210;
const SUB_H = 44;
const SERVER_TOP = 90;
const MEMBER_H = 44;
// The share group's frame: its filter, then its members one under another.
const GROUP_H = 26 + SHARE_GROUP.members.length * (MEMBER_H + 8);
// The server reaches down past the last member, so every match leaves it straight across.
const SERVER_H = SUBSCRIPTIONS.length * (SUB_H + 12) + GROUP_H;

/*
 * MQTT routing: one publish, the server matching its topic against the filter of every
 * subscription it holds, a copy to each match, one member of a share group, and the retained
 * message of the topic kept for a subscription that joins later.
 */
export function topics(words: Words): {boxes: Box[]; links: Link[]} {
  const boxes: Box[] = [
    {
      id: 'publisher',
      x: 0,
      y: 0,
      w: 360,
      h: 58,
      kind: 'service',
      lines: [...lines(words, 'publisher'), TOPIC],
    },
    {
      id: 'server',
      x: 0,
      y: SERVER_TOP,
      w: SERVER_W,
      h: SERVER_H,
      kind: 'broker',
      lines: lines(words, 'server'),
    },
  ];
  const levels: Record<string, number> = {};
  SUBSCRIPTIONS.forEach((sub, index) => {
    const y = SERVER_TOP + index * (SUB_H + 12);
    levels[sub.id] = y + SUB_H / 2;
    boxes.push({
      id: sub.id,
      x: SUB_X,
      y,
      w: SUB_W,
      h: SUB_H,
      kind: matches(sub.filter, TOPIC) ? 'good' : 'step',
      lines: [sub.filter, ...lines(words, sub.id)],
    });
  });
  const groupY = SERVER_TOP + SUBSCRIPTIONS.length * (SUB_H + 12);
  boxes.push({
    id: 'share',
    x: SUB_X - 10,
    y: groupY,
    w: SUB_W + 10,
    h: GROUP_H,
    kind: 'group',
    lines: lines(words, 'share'),
    href: '/components/subscriptions/shared/',
  });
  SHARE_GROUP.members.forEach((member, index) => {
    const y = groupY + 26 + index * (MEMBER_H + 8);
    levels[member] = y + MEMBER_H / 2;
    boxes.push({
      id: member,
      x: SUB_X,
      y,
      w: SUB_W - 10,
      h: MEMBER_H,
      // The server hands each message to one member of the group.
      kind: index === 0 ? 'good' : 'step',
      lines: [SHARE_GROUP.filter, ...lines(words, member)],
    });
  });
  const retainedY = SERVER_TOP + SERVER_H + 28;
  boxes.push(
    {
      id: 'retained',
      x: 0,
      y: retainedY,
      w: 172,
      h: 58,
      kind: 'broker',
      lines: lines(words, 'retained'),
      href: '/components/publishing/retained/',
    },
    {
      id: 'later',
      x: 188,
      y: retainedY,
      w: 172,
      h: 58,
      kind: 'good',
      lines: lines(words, 'later'),
    },
    {
      id: 'dropped',
      x: 0,
      y: retainedY + 58 + 16,
      w: 360,
      h: 44,
      kind: 'note',
      lines: lines(words, 'dropped'),
    },
  );
  const along = (id: string): number => (levels[id]! - SERVER_TOP) / SERVER_H;
  const reached = [
    ...SUBSCRIPTIONS.filter((sub) => matches(sub.filter, TOPIC)).map((sub) => sub.id),
    ...(matches(SHARE_GROUP.filter, TOPIC) ? [SHARE_GROUP.members[0]] : []),
  ];
  const links: Link[] = [
    {
      from: 'publisher',
      to: 'server',
      exitAt: SERVER_W / 2 / 360,
      label: word(words, 'publishes'),
    },
    ...reached.map(
      (id): Link => ({from: 'server', to: id, exit: 'right', enter: 'left', exitAt: along(id)}),
    ),
    {from: 'server', to: 'retained', label: word(words, 'keeps')},
    {from: 'retained', to: 'later'},
  ];
  return {boxes, links};
}
