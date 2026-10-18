import {existsSync} from 'node:fs';
import {join} from 'node:path';
import {describe, expect, it} from 'vitest';
import {LAYOUTS, type DiagramName, type Layout} from './layouts';
import type {Words} from './words';

// Every word is its own key, so the test reads the layout and never a page's wording.
const words = new Proxy({}, {get: (_, key) => String(key)}) as Words;

// The docs are served from the site root, so a route names the page's file under `docs/`.
const DOCS = join(__dirname, '..', '..', '..', '..', 'docs');

const hrefs = (name: DiagramName): Record<string, string> => {
  const make: (words: Words) => Layout = LAYOUTS[name];
  return Object.fromEntries(
    make(words)
      .boxes.filter((box) => box.href !== undefined)
      .map((box) => [box.id, box.href!]),
  );
};

const page = (href: string): boolean => {
  const route = href.replace(/^\/|\/$/g, '');
  return ['.md', '.mdx', '/index.md', '/index.mdx'].some((ending) =>
    existsSync(join(DOCS, route + ending)),
  );
};

describe('a box that links to a page', () => {
  const every = (Object.keys(LAYOUTS) as DiagramName[]).flatMap((name) =>
    Object.entries(hrefs(name)).map(([box, href]) => ({name, box, href})),
  );

  // One test over every box, so the guard holds for whichever diagrams the registry has.
  it('names a page of the site', () => {
    for (const {name, box, href} of every) {
      // A root-relative route: the link prepends the language's base URL, so one route serves
      // every translation, and the site ends every route with a slash.
      expect(href, `${name}, ${box}`).toMatch(/^\/[a-z0-9/-]+\/$/);
      expect(page(href), `${name}, ${box}: no page under docs/ for ${href}`).toBe(true);
    }
  });
});

describe('a box that names a component', () => {
  it('links to that component page in the model of MQTT', () => {
    expect(hrefs('topics')).toEqual({
      share: '/components/subscriptions/shared/',
      retained: '/components/publishing/retained/',
    });
    expect(hrefs('qos')).toEqual({
      qos0: '/components/subscriptions/qos/',
      qos1: '/components/settlement/',
      qos2: '/components/settlement/',
    });
    expect(hrefs('sessions')).toEqual({
      connect: '/components/broker/session/',
      ping: '/components/broker/reconnect/',
      will: '/components/broker/last-will/',
      reconnect: '/components/broker/reconnect/',
    });
  });

  it('links to that component page in the design of the crate', () => {
    expect(hrefs('design')).toEqual({
      subscriber: '/architecture/path/',
      publishers: '/components/publishing/',
      context: '/components/context/',
      inProcess: '/components/testing/',
      broker: '/components/broker/',
      descriptors: '/components/subscriptions/',
      task: '/architecture/demultiplexing/',
      server: '/architecture/mqtt/',
    });
  });

  it('links to that component page in the lifecycle', () => {
    expect(hrefs('lifecycle')).toEqual({
      unconnected: '/components/broker/',
      closed: '/components/broker/shutdown/',
      options: '/components/broker/errors/',
      dial: '/components/broker/startup/',
      subscribe: '/components/subscriptions/',
      running: '/components/broker/reconnect/',
      disconnect: '/components/broker/shutdown/',
      afterShutdown: '/components/broker/shutdown/',
    });
  });

  it('links to that component page on the path of a message', () => {
    expect(hrefs('path')).toEqual({
      demux: '/architecture/demultiplexing/',
      message: '/components/context/',
      dispatch: '/architecture/demultiplexing/',
      handler: '/components/subscriptions/',
      server: '/architecture/mqtt/',
      ack: '/components/settlement/',
      publisher: '/components/publishing/',
    });
  });

  it('links to that component page in the demultiplexing and the lanes', () => {
    expect(hrefs('demultiplexing')).toEqual({registry: '/components/subscriptions/overlaps/'});
    expect(hrefs('lanes')).toEqual({scale: '/components/subscriptions/shared/'});
  });

  it('links to that component page in the settlement and the retries', () => {
    expect(hrefs('settlement')).toEqual({
      retry: '/components/settlement/retry/',
      retryCapped: '/components/settlement/retries/',
      retryAfter: '/components/settlement/retry-after/',
      qos0: '/components/subscriptions/qos/',
    });
    expect(hrefs('retries')).toEqual({
      outcome: '/components/settlement/retry-after/',
      cap: '/components/settlement/retries/',
      topicCopy: '/components/subscriptions/mqtt-topic/',
      filterCopy: '/components/settlement/out-retry/',
      deadLetter: '/components/settlement/dead-letters/',
    });
  });
});
