import fs from 'node:fs';
import path from 'node:path';

/**
 * What a build documents, read from the environment the deploy script sets.
 *
 * - `latest`: the newest release, the default; it carries no banner.
 * - `next`: the documentation of `main`, ahead of every release; its pages say so and are
 *   kept out of search engines.
 * - `archive`: an older release; its pages point the reader at `latest` and are kept out of
 *   search engines, so a search lands on the release a reader should use.
 */
export type VersionKind = 'latest' | 'next' | 'archive';

const kinds: readonly VersionKind[] = ['latest', 'next', 'archive'];

export interface BuildVersion {
  /** `/ruststream-rumqttc/<directory>/`. */
  baseUrl: string;
  /** The directory of the site root the build is served from: `0.7`, `latest`, `next`. */
  directory: string;
  kind: VersionKind;
  /**
   * The pages come from an archive checkout: one language, no landing page, the archive's own
   * sidebar and redirects.
   */
  archive: boolean;
  /** The checkout holding `docs/`, the sources the pages include, and an archive's sidebar. */
  contentRoot: string;
  /** The git ref the excerpts' source links point at. */
  sourceRef: string;
  /** Every page asks search engines to leave it out of their results. */
  noIndex: boolean;
  /**
   * The base URL of the copy search engines should index: `/ruststream-rumqttc/latest/` for the
   * newest
   * line, which is built twice (under its number and under `latest`), the build's own base URL
   * otherwise. A page's canonical URL and its language alternates are written against it.
   */
  canonicalBaseUrl: string;
  /** The build publishes a sitemap: only the copy that is indexed under its own address does. */
  sitemap: boolean;
  /** The docs plugin's options for the one version a build holds. */
  docsVersion: {label: string; banner: 'none' | 'unreleased' | 'unmaintained'};
  /**
   * The crate's rustdoc on docs.rs for the code this build documents, with a trailing slash:
   * the minor line of a release (docs.rs serves its newest patch), and the newest release for
   * `next`, whose code is not published yet.
   */
  docsRs: string;
  /**
   * Where an API link finds each crate it may name, with a trailing slash: this crate at
   * `docsRs`, and the core at the minor line the checkout's lock file pins, whose rustdoc is the
   * one built beside this crate's.
   */
  apiDocs: Record<string, string>;
}

export const siteRoot = '/ruststream-rumqttc/';

/** The landing page of the core's site in `locale`: the navbar's brand leads there. */
export function coreLanding(locale: string): string {
  const root = 'https://powersemmi.github.io/ruststream/latest/';
  return locale === 'en' ? root : `${root}${locale}/`;
}

// The core release a checkout builds against, as its lock file records it.
const lockedCore = /^name = "ruststream"\nversion = "(\d+\.\d+)\.\d+[^"]*"$/m;

function coreLine(contentRoot: string): string {
  const lock = path.join(contentRoot, 'Cargo.lock');
  const line = fs.existsSync(lock) ? lockedCore.exec(fs.readFileSync(lock, 'utf8'))?.[1] : undefined;
  return line ?? 'latest';
}

const banners = {latest: 'none', next: 'unreleased', archive: 'unmaintained'} as const;

export function readBuildVersion(
  env: Record<string, string | undefined>,
  repoRoot: string,
): BuildVersion {
  const baseUrl = env.DOCS_BASE_URL ?? `${siteRoot}latest/`;
  const directory = baseUrl.startsWith(siteRoot)
    ? /^([^/]+)\/$/.exec(baseUrl.slice(siteRoot.length))?.[1]
    : undefined;
  if (!directory) {
    throw new Error(`DOCS_BASE_URL must be ${siteRoot}<version>/, got '${baseUrl}'`);
  }
  const archive = Boolean(env.DOCS_CONTENT_ROOT);
  const requested = env.DOCS_VERSION_KIND ?? (archive ? 'archive' : 'latest');
  if (!kinds.includes(requested as VersionKind)) {
    throw new Error(`DOCS_VERSION_KIND must be one of ${kinds.join(', ')}, got '${requested}'`);
  }
  const kind = requested as VersionKind;
  // The deploy script names the minor line of a release; a build without it (a local one, or
  // CI's check) documents the newest release.
  const line = env.DOCS_LINE || (/^\d+\.\d+$/.test(directory) ? directory : undefined);
  if (line && !/^\d+\.\d+$/.test(line)) {
    throw new Error(`DOCS_LINE must be a minor line such as 0.7, got '${line}'`);
  }
  // An archive checkout holds an old line's pages; it never becomes `latest` or `next`.
  if (archive && kind !== 'archive') {
    throw new Error(`DOCS_CONTENT_ROOT names an archive, which builds as archive, not '${kind}'`);
  }
  const noIndex = kind !== 'latest';
  const canonicalBaseUrl = kind === 'latest' ? `${siteRoot}latest/` : baseUrl;
  const contentRoot = archive ? path.resolve(repoRoot, env.DOCS_CONTENT_ROOT!) : repoRoot;
  const docsRs = `https://docs.rs/ruststream-rumqttc/${kind === 'next' ? 'latest' : (line ?? 'latest')}/`;
  return {
    baseUrl,
    directory,
    kind,
    archive,
    contentRoot,
    sourceRef: env.DOCS_SOURCE_REF ?? 'main',
    noIndex,
    canonicalBaseUrl,
    sitemap: !noIndex && canonicalBaseUrl === baseUrl,
    docsVersion: {label: directory, banner: banners[kind]},
    docsRs,
    apiDocs: {
      ruststream_rumqttc: docsRs,
      ruststream: `https://docs.rs/ruststream/${coreLine(contentRoot)}/`,
    },
  };
}
