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
