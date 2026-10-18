import type {Box, BoxKind, Link} from './flow';
import {lines, word, type Words} from './words';

const COLUMN = 156;
const RIGHT = 180;
const ROW = 72;
const HEIGHT = 58;

/*
 * The path of one message that is answered, as a loop: down the left column from the socket to
 * the handler, then up the right column, the reply and then the acknowledgement, through the
 * client's request channel and back onto the wire.
 */
export function path(words: Words): {boxes: Box[]; links: Link[]} {
  const cell = (
    id: string,
    x: number,
    row: number,
    kind: BoxKind,
    href?: string,
  ): Box => ({
    id,
    x,
    y: row * ROW,
    w: COLUMN,
    h: HEIGHT,
    kind,
    lines: lines(words, id),
    href,
  });
  const boxes: Box[] = [
    cell('poll', 0, 0, 'broker'),
    cell('demux', 0, 1, 'broker', '/architecture/demultiplexing/'),
    cell('message', 0, 2, 'broker', '/components/context/'),
    cell('dispatch', 0, 3, 'core', '/architecture/demultiplexing/'),
    cell('decode', 0, 4, 'step'),
    cell('handler', 0, 5, 'service', '/components/subscriptions/'),
    cell('server', RIGHT, 0, 'broker', '/architecture/mqtt/'),
    cell('write', RIGHT, 1, 'broker'),
    cell('requests', RIGHT, 2, 'broker'),
    cell('ack', RIGHT, 3, 'good', '/components/settlement/'),
    cell('publisher', RIGHT, 4, 'broker', '/components/publishing/'),
    cell('encode', RIGHT, 5, 'step'),
  ];
  const links: Link[] = [
    {from: 'server', to: 'poll'},
    {from: 'poll', to: 'demux'},
    {from: 'demux', to: 'message'},
    {from: 'message', to: 'dispatch'},
    {from: 'dispatch', to: 'decode'},
    {from: 'decode', to: 'handler'},
    {from: 'handler', to: 'encode'},
    {from: 'encode', to: 'publisher'},
    {from: 'publisher', to: 'requests', exit: 'right', enter: 'right'},
    {from: 'publisher', to: 'ack', label: word(words, 'then')},
    {from: 'ack', to: 'requests'},
    {from: 'requests', to: 'write'},
    {from: 'write', to: 'server'},
  ];
  return {boxes, links};
}
