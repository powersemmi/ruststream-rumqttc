/*
 * The sections of the earlier benchmarks page, by the anchor links to them still carry, and
 * where each one lives now. A static site cannot redirect an anchor, so the page forwards one
 * itself.
 */

const MOVED: Record<string, string> = {
  'the-numbers': '#results',
  'the-crates-own-code': '#cost-of-the-code',
  'what-they-do-not-mean': 'numbers/#what-they-do-not-mean',
  'running-it-yourself': 'methodology/#running-it-yourself',
};

/**
 * Where, relative to the Benchmarks page, the section `hash` named on the earlier page lives
 * now, or `undefined` for an anchor the page still has.
 */
export function movedSection(hash: string): string | undefined {
  const anchor = hash.replace(/^#/, '');
  // An own property only: an anchor named like a member of every object is not a section.
  return Object.hasOwn(MOVED, anchor) ? MOVED[anchor] : undefined;
}
