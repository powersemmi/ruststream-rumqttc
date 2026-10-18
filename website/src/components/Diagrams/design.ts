import type {Box, Link} from './flow';
import {lines, word, type Words} from './words';

/*
 * Where the crate sits: the service on top, the core's runtime under it, this crate between the
 * runtime and the rumqttc client, and the server at the bottom.
 */
export function design(words: Words): {boxes: Box[]; links: Link[]} {
  const box = (
    id: string,
    x: number,
    y: number,
    w: number,
    h: number,
    fields: Partial<Box> = {},
  ): Box => ({id, x, y, w, h, kind: 'step', lines: lines(words, id), ...fields});
  // The crate's columns leave a gutter at each side, where a link from a box to the client under
  // it runs past the boxes in between.
  const left = 28;
  const right = 182;
  const column = 150;
  const centre = (x: number): number => (x + column / 2 - 12) / 336;
  // Every subscription, publisher and acknowledgement reaches the server through the one client
  // pair below the crate: the cloneable handle takes requests, and the event loop, polled by the
  // crate's connection task, writes them and reads what the server sends.
  const boxes: Box[] = [
    box('service', 0, 0, 360, 80, {kind: 'group'}),
    box('handlers', 12, 26, 160, 42, {kind: 'service', code: true}),
    box('app', 188, 26, 160, 42, {kind: 'service', code: true}),
    box('core', 0, 104, 360, 72, {kind: 'group'}),
    box('runtime', 12, 128, 336, 36, {kind: 'core'}),
    box('crate', 0, 200, 360, 270, {kind: 'group'}),
    box('subscriber', left, 226, column, 58, {
      kind: 'broker',
      code: true,
      href: '/architecture/path/',
    }),
    box('publishers', right, 226, column, 58, {
      kind: 'broker',
      code: true,
      href: '/components/publishing/',
    }),
    box('context', left, 294, column, 30, {kind: 'broker', href: '/components/context/'}),
    box('inProcess', right, 294, column, 30, {kind: 'broker', href: '/components/testing/'}),
    box('broker', left, 334, column, 58, {kind: 'broker', code: true, href: '/components/broker/'}),
    box('descriptors', right, 334, column, 58, {
      kind: 'broker',
      code: true,
      href: '/components/subscriptions/',
    }),
    box('task', left, 402, column, 58, {kind: 'broker', href: '/architecture/demultiplexing/'}),
    box('client', 0, 494, 360, 82, {kind: 'group'}),
    box('eventLoop', left, 520, column, 44, {kind: 'broker', code: true}),
    box('asyncClient', 202, 520, 130, 44, {kind: 'broker', code: true}),
    box('server', 12, 600, 336, 36, {kind: 'broker', href: '/architecture/mqtt/'}),
  ];
  const links: Link[] = [
    {from: 'handlers', to: 'runtime', enterAt: 80 / 336, label: word(words, 'mounted')},
    {from: 'runtime', to: 'subscriber', exitAt: centre(left), label: word(words, 'polls')},
    {from: 'task', to: 'subscriber', exit: 'left', enter: 'left'},
    {from: 'task', to: 'eventLoop', label: word(words, 'drives')},
    {from: 'publishers', to: 'asyncClient', exit: 'right', enter: 'right'},
    {from: 'asyncClient', to: 'eventLoop'},
    {from: 'eventLoop', to: 'server', exitAt: 0.5, enterAt: centre(left)},
  ];
  return {boxes, links};
}
