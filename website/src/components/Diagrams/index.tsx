import type {ReactNode} from 'react';
import {Flow} from './Flow';
import {LAYOUTS, type DiagramName, type Layout} from './layouts';
import {word, type Words} from './words';

/*
 * The diagrams of the Architecture pages. A layout places the boxes; the page supplies every word,
 * so a translation changes the words and never the drawing.
 */

export type {DiagramName};

export function Diagram({name, words}: {name: DiagramName; words: Words}): ReactNode {
  // Typed through the layout's signature, so the registry may hold any set of diagrams.
  const make: (words: Words) => Layout = LAYOUTS[name];
  const layout = make(words);
  return <Flow title={word(words, 'title')} text={word(words, 'text')} {...layout} />;
}
