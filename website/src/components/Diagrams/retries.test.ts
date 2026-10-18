import {expect, it} from 'vitest';
import {retries} from './retries';
import type {Words} from './words';

const words = new Proxy({}, {get: (_, key) => String(key)}) as Words;

it('decides on the attempt cap before a retry goes anywhere', () => {
  const {boxes, links} = retries(words);
  const from = (id: string): string[] =>
    links.filter((link) => link.from === id).map((link) => link.to);
  expect(from('delivery')).toEqual(['outcome']);
  expect(from('outcome')).toEqual(['cap']);
  expect(from('cap')).toEqual(['below', 'atCap']);
  // Below the cap the original is acknowledged and a copy goes to the address of the
  // subscription's form; at the cap the delivery leaves for good.
  const inside = (frame: string): string[] => {
    const outer = boxes.find((box) => box.id === frame)!;
    return boxes
      .filter(
        (box) =>
          box.id !== frame &&
          box.x >= outer.x &&
          box.y >= outer.y &&
          box.x + box.w <= outer.x + outer.w &&
          box.y + box.h <= outer.y + outer.h,
      )
      .map((box) => box.id);
  };
  expect(inside('below')).toEqual(['acked', 'topicCopy', 'filterCopy']);
  expect(inside('atCap')).toEqual(['deadLetter', 'rejected']);
});
