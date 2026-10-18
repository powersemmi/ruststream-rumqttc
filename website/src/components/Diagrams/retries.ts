import type {Box, Link} from './flow';
import {lines, word, type Words} from './words';

/*
 * A retry on MQTT: the runtime counts the attempts on its own header, because the protocol counts
 * none, and the cap decides. Below it, the original is acknowledged and a copy is published to the
 * address of the subscription's form; at it, the delivery goes to the dead-letter topic or is
 * acknowledged and let go.
 */
export function retries(words: Words): {boxes: Box[]; links: Link[]} {
  const box = (
    id: string,
    x: number,
    y: number,
    w: number,
    h: number,
    fields: Partial<Box> = {},
  ): Box => ({id, x, y, w, h, kind: 'step', lines: lines(words, id), ...fields});
  const boxes: Box[] = [
    box('delivery', 12, 0, 336, 44, {kind: 'broker'}),
    box('outcome', 12, 72, 336, 44, {kind: 'service', href: '/components/settlement/retry-after/'}),
    box('cap', 12, 144, 336, 44, {kind: 'core', href: '/components/settlement/retries/'}),
    box('below', 0, 220, 172, 250, {kind: 'group'}),
    box('acked', 12, 246, 148, 58, {kind: 'broker'}),
    box('topicCopy', 12, 316, 148, 58, {
      kind: 'step',
      href: '/components/subscriptions/mqtt-topic/',
    }),
    box('filterCopy', 12, 386, 148, 72, {kind: 'step', href: '/components/settlement/out-retry/'}),
    box('atCap', 188, 220, 172, 250, {kind: 'group'}),
    box('deadLetter', 200, 246, 148, 72, {
      kind: 'bad',
      href: '/components/settlement/dead-letters/',
    }),
    box('rejected', 200, 330, 148, 72, {kind: 'bad'}),
    box('fanOut', 0, 490, 360, 74, {kind: 'note'}),
  ];
  const links: Link[] = [
    {from: 'delivery', to: 'outcome', label: word(words, 'handled')},
    {from: 'outcome', to: 'cap', label: word(words, 'retries')},
    {from: 'cap', to: 'below', exitAt: (86 - 12) / 336, label: word(words, 'belowCap')},
    {from: 'cap', to: 'atCap', exitAt: (274 - 12) / 336, label: word(words, 'reached')},
  ];
  return {boxes, links};
}
