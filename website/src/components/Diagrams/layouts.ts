import {demultiplexing} from './demultiplexing';
import {design} from './design';
import type {Box, Lane, Link} from './flow';
import {lanes} from './lanes';
import {lifecycle} from './lifecycle';
import {path} from './path';
import {qos} from './qos';
import {retries} from './retries';
import {sessions} from './sessions';
import {settlement} from './settlement';
import {topics} from './topics';
import type {Words} from './words';

export interface Layout {
  boxes: Box[];
  links: Link[];
  lanes?: Lane[];
}

/** Every diagram a page can name: a layout places the boxes, the page supplies the words. */
export const LAYOUTS = {
  topics,
  qos,
  sessions,
  design,
  lifecycle,
  path,
  demultiplexing,
  lanes,
  settlement,
  retries,
} satisfies Record<string, (words: Words) => Layout>;

export type DiagramName = keyof typeof LAYOUTS;
