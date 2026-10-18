import fs from 'node:fs/promises';
import {existsSync} from 'node:fs';
import path from 'node:path';
import type {LoadContext, Plugin} from '@docusaurus/types';

/**
 * Serves files of the repository at fixed addresses of a build, beside the pages.
 *
 * The benchmark results document is written by the benchmark recipes into `docs/benchmarks/`,
 * and other sites read it from there; its address on this site, `<version>/benchmarks/
 * results.json`, is published as its machine-readable form. The source stays where the recipes
 * write it.
 */
export interface Options {
  /** The checkout the sources are read from. */
  rootDir: string;
  /** Published path (relative to the build's base URL) to its source, relative to `rootDir`. */
  files: Record<string, string>;
}

/** Copies each source into `outDir` at its published path; a source that is absent is skipped. */
export async function publishFiles(
  rootDir: string,
  outDir: string,
  files: Record<string, string>,
): Promise<void> {
  for (const [published, source] of Object.entries(files)) {
    const from = path.join(rootDir, source);
    if (!existsSync(from)) {
      continue;
    }
    const target = path.join(outDir, published);
    await fs.mkdir(path.dirname(target), {recursive: true});
    await fs.copyFile(from, target);
  }
}

export default function publishFilesPlugin(context: LoadContext, options: Options): Plugin {
  return {
    name: 'ruststream-publish-files',
    async postBuild({outDir}) {
      // The files belong to the version, not to a language: only the default one serves them.
      if (context.i18n.currentLocale === context.i18n.defaultLocale) {
        await publishFiles(options.rootDir, outDir, options.files);
      }
    },
  };
}
