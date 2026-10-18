import type {Box, Link} from './flow';
import {lines, word, type Words} from './words';

// The right column's steps, one row each; a state on the left sits level with the step that
// makes it.
const ROW = 70;
const TOP = 28;
const rowY = (row: number): number => TOP + row * ROW;

/*
 * The broker's three states as types on the left, and on the right what each transition does on
 * MQTT: the options checked before any I/O, the connection task and its first CONNACK, a
 * SUBSCRIBE for each subscription, the running connection, and the DISCONNECT at shutdown.
 */
export function lifecycle(words: Words): {boxes: Box[]; links: Link[]} {
  const left = (id: string, row: number, href?: string): Box => ({
    id,
    x: 12,
    y: rowY(row),
    w: 156,
    h: 58,
    kind: 'broker',
    code: true,
    lines: lines(words, id),
    href,
  });
  const right = (id: string, row: number, href?: string): Box => ({
    id,
    x: 192,
    y: rowY(row),
    w: 156,
    h: 58,
    kind: 'core',
    lines: lines(words, id),
    href,
  });
  const height = rowY(5) + 58 + 12;
  const boxes: Box[] = [
    {id: 'states', x: 0, y: 0, w: 176, h: height, kind: 'group', lines: lines(words, 'states')},
    {id: 'mqtt', x: 184, y: 0, w: 176, h: height, kind: 'group', lines: lines(words, 'mqtt')},
    left('unconnected', 0, '/components/broker/'),
    left('connected', 3),
    left('closed', 5, '/components/broker/shutdown/'),
    right('config', 0),
    right('options', 1, '/components/broker/errors/'),
    right('dial', 2, '/components/broker/startup/'),
    right('subscribe', 3, '/components/subscriptions/'),
    right('running', 4, '/components/broker/reconnect/'),
    right('disconnect', 5, '/components/broker/shutdown/'),
    {
      id: 'afterShutdown',
      x: 0,
      y: height + 22,
      w: 360,
      h: 44,
      kind: 'note',
      lines: lines(words, 'afterShutdown'),
      href: '/components/broker/shutdown/',
    },
  ];
  const links: Link[] = [
    {from: 'unconnected', to: 'connected', label: word(words, 'connect')},
    {from: 'connected', to: 'closed', label: word(words, 'shutdown')},
    {from: 'config', to: 'options'},
    {from: 'options', to: 'dial'},
    {from: 'dial', to: 'subscribe'},
    {from: 'subscribe', to: 'running'},
    {from: 'running', to: 'disconnect'},
  ];
  return {boxes, links};
}
