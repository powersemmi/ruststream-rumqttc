// Pages of the earlier site, by their address within a version. Every language of a build gets
// the same set.

/**
 * Old address to its target, relative to the build's base URL. `docsRs` is the crate's rustdoc
 * for the version built, with a trailing slash.
 */
export function redirects(_docsRs: string): Record<string, string> {
  return {};
}
