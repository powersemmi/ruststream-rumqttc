import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import {afterEach, beforeEach, describe, expect, it} from 'vitest';
import {publishFiles} from './publishFiles';

let root: string;

beforeEach(() => {
  root = fs.mkdtempSync(path.join(os.tmpdir(), 'publish-files-'));
  fs.mkdirSync(path.join(root, 'docs', 'benchmarks'), {recursive: true});
  fs.writeFileSync(path.join(root, 'docs', 'benchmarks', 'results.json'), '{"schema":3}');
});

afterEach(() => {
  fs.rmSync(root, {recursive: true, force: true});
});

describe('publishFiles', () => {
  it('copies each file to its published path in the build', async () => {
    const outDir = path.join(root, 'build');
    await publishFiles(root, outDir, {'benchmarks/results.json': 'docs/benchmarks/results.json'});
    expect(fs.readFileSync(path.join(outDir, 'benchmarks', 'results.json'), 'utf8')).toBe(
      '{"schema":3}',
    );
  });

  it('skips a file the checkout does not have, as an archive may not', async () => {
    const outDir = path.join(root, 'build');
    await publishFiles(root, outDir, {'benchmarks/other.json': 'docs/benchmarks/other.json'});
    expect(fs.existsSync(path.join(outDir, 'benchmarks', 'other.json'))).toBe(false);
  });
});
