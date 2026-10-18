import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import {describe, expect, it} from 'vitest';
import {coreLanding, readBuildVersion} from './versioning';

const repoRoot = '/work/ruststream-rumqttc';

describe('readBuildVersion', () => {
  it('builds the repository itself as the latest release by default', () => {
    expect(readBuildVersion({}, repoRoot)).toEqual({
      baseUrl: '/ruststream-rumqttc/latest/',
      directory: 'latest',
      kind: 'latest',
      archive: false,
      contentRoot: repoRoot,
      sourceRef: 'main',
      noIndex: false,
      canonicalBaseUrl: '/ruststream-rumqttc/latest/',
      sitemap: true,
      docsVersion: {label: 'latest', banner: 'none'},
      docsRs: 'https://docs.rs/ruststream-rumqttc/latest/',
      apiDocs: {
        ruststream_rumqttc: 'https://docs.rs/ruststream-rumqttc/latest/',
        ruststream: 'https://docs.rs/ruststream/latest/',
      },
    });
  });

  it('links the core items to the core line the checkout locks', () => {
    const checkout = fs.mkdtempSync(path.join(os.tmpdir(), 'versioning-'));
    try {
      fs.writeFileSync(
        path.join(checkout, 'Cargo.lock'),
        [
          '[[package]]',
          'name = "ruststream-macros"',
          'version = "0.8.4"',
          '',
          '[[package]]',
          'name = "ruststream"',
          'version = "0.7.3"',
          'source = "registry+https://github.com/rust-lang/crates.io-index"',
          '',
        ].join('\n'),
      );
      const version = readBuildVersion(
        {DOCS_BASE_URL: '/ruststream-rumqttc/next/', DOCS_VERSION_KIND: 'next'},
        checkout,
      );
      expect(version.apiDocs).toEqual({
        ruststream_rumqttc: 'https://docs.rs/ruststream-rumqttc/latest/',
        ruststream: 'https://docs.rs/ruststream/0.7/',
      });
    } finally {
      fs.rmSync(checkout, {recursive: true, force: true});
    }
  });

  it('links a release to its minor line on docs.rs, under its number and under latest', () => {
    for (const directory of ['0.7', 'latest']) {
      const version = readBuildVersion(
        {DOCS_BASE_URL: `/ruststream-rumqttc/${directory}/`, DOCS_VERSION_KIND: 'latest', DOCS_LINE: '0.7'},
        repoRoot,
      );
      expect(version.docsRs).toBe('https://docs.rs/ruststream-rumqttc/0.7/');
    }
  });

  it('names latest as the indexed copy of the newest line under its number', () => {
    const numbered = readBuildVersion(
      {DOCS_BASE_URL: '/ruststream-rumqttc/0.7/', DOCS_VERSION_KIND: 'latest', DOCS_LINE: '0.7'},
      repoRoot,
    );
    expect(numbered.noIndex).toBe(false);
    expect(numbered.canonicalBaseUrl).toBe('/ruststream-rumqttc/latest/');
    expect(numbered.sitemap).toBe(false);
  });

  it('links next to the newest release on docs.rs', () => {
    const version = readBuildVersion(
      {DOCS_BASE_URL: '/ruststream-rumqttc/next/', DOCS_VERSION_KIND: 'next', DOCS_LINE: ''},
      repoRoot,
    );
    expect(version.docsRs).toBe('https://docs.rs/ruststream-rumqttc/latest/');
  });

  it('refuses a line that is not a minor version', () => {
    expect(() =>
      readBuildVersion({DOCS_BASE_URL: '/ruststream-rumqttc/0.7/', DOCS_LINE: '0.7.1'}, repoRoot),
    ).toThrow(/DOCS_LINE/);
  });

  it('marks next as unreleased and keeps it out of search engines', () => {
    const version = readBuildVersion(
      {DOCS_BASE_URL: '/ruststream-rumqttc/next/', DOCS_VERSION_KIND: 'next'},
      repoRoot,
    );
    expect(version.directory).toBe('next');
    expect(version.archive).toBe(false);
    expect(version.noIndex).toBe(true);
    expect(version.canonicalBaseUrl).toBe('/ruststream-rumqttc/next/');
    expect(version.sitemap).toBe(false);
    expect(version.docsVersion).toEqual({label: 'next', banner: 'unreleased'});
  });

  it('marks an older release as unmaintained and keeps it out of search engines', () => {
    const version = readBuildVersion(
      {DOCS_BASE_URL: '/ruststream-rumqttc/0.6/', DOCS_VERSION_KIND: 'archive'},
      repoRoot,
    );
    expect(version.noIndex).toBe(true);
    expect(version.canonicalBaseUrl).toBe('/ruststream-rumqttc/0.6/');
    expect(version.sitemap).toBe(false);
    expect(version.docsVersion).toEqual({label: '0.6', banner: 'unmaintained'});
    expect(version.contentRoot).toBe(repoRoot);
    expect(version.docsRs).toBe('https://docs.rs/ruststream-rumqttc/0.6/');
  });

  it('reads an archive from its own checkout, relative to the repository', () => {
    const version = readBuildVersion(
      {
        DOCS_BASE_URL: '/ruststream-rumqttc/0.5/',
        DOCS_CONTENT_ROOT: '../archive',
        DOCS_SOURCE_REF: 'docs/archive/v0.5',
      },
      repoRoot,
    );
    expect(version.archive).toBe(true);
    expect(version.kind).toBe('archive');
    expect(version.contentRoot).toBe(path.resolve(repoRoot, '../archive'));
    expect(version.sourceRef).toBe('docs/archive/v0.5');
    expect(version.docsVersion.banner).toBe('unmaintained');
  });

  it('refuses an archive checkout built as latest or next', () => {
    for (const kind of ['latest', 'next']) {
      expect(() =>
        readBuildVersion(
          {DOCS_BASE_URL: '/ruststream-rumqttc/0.5/', DOCS_CONTENT_ROOT: '/a', DOCS_VERSION_KIND: kind},
          repoRoot,
        ),
      ).toThrow(/archive/);
    }
  });

  it('refuses an unknown kind and a base URL outside the site', () => {
    expect(() => readBuildVersion({DOCS_VERSION_KIND: 'beta'}, repoRoot)).toThrow(/beta/);
    expect(() => readBuildVersion({DOCS_BASE_URL: '/other/0.7/'}, repoRoot)).toThrow(
      /DOCS_BASE_URL/,
    );
  });
});

describe('coreLanding', () => {
  it('opens the landing page of the core site in the same language', () => {
    expect(coreLanding('en')).toBe('https://powersemmi.github.io/ruststream/latest/');
    expect(coreLanding('ru')).toBe('https://powersemmi.github.io/ruststream/latest/ru/');
    expect(coreLanding('zh')).toBe('https://powersemmi.github.io/ruststream/latest/zh/');
  });
});
