import { access, mkdtemp, readFile, rm, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join, resolve } from 'node:path';
import { afterEach, beforeEach, describe, expect, it } from 'vitest';
import { importCsv, type ImportOptions } from '../src/import.ts';
import type { Checkpoint } from '../src/checkpoint.ts';
import type { Transaction } from '../src/schema.ts';
import type { CheckpointStore, RowSink } from '../src/sinks/types.ts';
import { csvRow, HEADER, uuid } from './helpers.ts';

/** Counts how often each id is written, and can simulate a crash on the n-th batch. */
class CountingSink implements RowSink {
  readonly writes: Map<string, number>;
  readonly #crashOnBatch: number | undefined;
  #batches = 0;

  constructor(writes: Map<string, number>, crashOnBatch?: number) {
    this.writes = writes;
    this.#crashOnBatch = crashOnBatch;
  }
  writeBatch(rows: Transaction[]) {
    if (++this.#batches === this.#crashOnBatch) throw new Error('simulated crash');
    for (const r of rows) this.writes.set(r.id, (this.writes.get(r.id) ?? 0) + 1);
  }
  close() {}
}

/**
 * Commits rows and checkpoint together, like PostgresSink, and can crash right *after* a commit:
 * the moment where a file checkpoint would still point at the previous batch.
 */
class TransactionalSink implements RowSink {
  readonly writes: Map<string, number>;
  readonly saved: Map<string, Checkpoint>;
  readonly checkpoints: CheckpointStore;
  readonly #crashAfterBatch: number | undefined;
  #batches = 0;

  constructor(
    writes: Map<string, number>,
    saved: Map<string, Checkpoint>,
    crashAfterBatch?: number,
  ) {
    this.writes = writes;
    this.saved = saved;
    this.#crashAfterBatch = crashAfterBatch;
    this.checkpoints = {
      load: (key) => Promise.resolve(saved.get(key)),
      clear: (key) => Promise.resolve(void saved.delete(key)),
    };
  }
  writeBatch(rows: Transaction[], checkpoint?: Checkpoint) {
    for (const r of rows) this.writes.set(r.id, (this.writes.get(r.id) ?? 0) + 1);
    if (checkpoint) this.saved.set(checkpoint.filePath, structuredClone(checkpoint));
    if (++this.#batches === this.#crashAfterBatch) throw new Error('crash after commit');
  }
  close() {}
}

// Physical lines: header=1, v0=2, v1=3-4 (multi-line), v2=5, bad=6, v3..v7=7..11, bad=12, v8=13, v9=14
const LINES = [
  HEADER,
  csvRow(uuid(0)),
  csvRow(uuid(1), { description: '"Refund\nsee ticket"' }),
  csvRow(uuid(2)),
  csvRow(uuid(100), { amount: 'bad' }),
  ...[3, 4, 5, 6, 7].map((n) => csvRow(uuid(n))),
  csvRow(uuid(101), { amount: 'bad' }),
  csvRow(uuid(8)),
  csvRow(uuid(9)),
];

let dir: string;
let base: Omit<ImportOptions, 'sink'>;
beforeEach(async () => {
  dir = await mkdtemp(join(tmpdir(), 'csv-resume-'));
  const filePath = join(dir, 'input.csv');
  await writeFile(filePath, `\uFEFF${LINES.join('\n')}\n`);
  base = {
    filePath,
    rejectsPath: join(dir, 'rejects.ndjson'),
    batchSize: 3,
    maxInvalidRows: 10,
    checkpoint: { path: join(dir, 'checkpoint.json'), target: 'test' },
  };
});
afterEach(async () => {
  await rm(dir, { recursive: true, force: true });
});

const exists = (path: string) =>
  access(path).then(
    () => true,
    () => false,
  );

describe('resume from checkpoint', () => {
  it('continues after the last committed batch and writes every row exactly once', async () => {
    const writes = new Map<string, number>();

    // Batches 1 and 2 (v0-v5) commit, batch 3 crashes.
    await expect(importCsv({ ...base, sink: new CountingSink(writes, 3) })).rejects.toThrow(
      'simulated crash',
    );
    expect(writes.size).toBe(6);
    expect(await exists(base.checkpoint!.path)).toBe(true);

    const result = await importCsv({ ...base, sink: new CountingSink(writes) });

    expect(result).toMatchObject({ resumed: true, processed: 12, invalid: 2, written: 10 });
    expect([...writes.values()].every((n) => n === 1)).toBe(true);
    expect(writes.size).toBe(10);
    expect(await exists(base.checkpoint!.path)).toBe(false);

    const rejects = (await readFile(base.rejectsPath, 'utf8'))
      .trim()
      .split('\n')
      .map((l) => JSON.parse(l));
    expect(rejects.map((r) => [r.line, r.raw.id])).toEqual([
      [6, uuid(100)],
      [12, uuid(101)],
    ]);
  });

  it('never replays a batch when the sink commits the checkpoint with the rows', async () => {
    const writes = new Map<string, number>();
    const saved = new Map<string, Checkpoint>();

    // Valid rows in batches of 3: [v0-v2] [v3-v5] [v6-v8] [v9]. Batch 3 commits, then the process dies.
    await expect(
      importCsv({ ...base, sink: new TransactionalSink(writes, saved, 3) }),
    ).rejects.toThrow('crash after commit');
    expect(writes.size).toBe(9);
    expect(saved.get(resolve(base.filePath))).toMatchObject({ counters: { written: 9 } });

    const result = await importCsv({ ...base, sink: new TransactionalSink(writes, saved) });

    expect(result).toMatchObject({ resumed: true, processed: 12, invalid: 2, written: 10 });
    expect([...writes.values()].every((n) => n === 1)).toBe(true);
    expect(writes.size).toBe(10);
    expect(saved.size).toBe(0);
    // The sink owns the checkpoint, so no file is written.
    expect(await exists(base.checkpoint!.path)).toBe(false);

    const rejects = (await readFile(base.rejectsPath, 'utf8'))
      .trim()
      .split('\n')
      .map((l) => JSON.parse(l));
    expect(rejects.map((r) => [r.line, r.raw.id])).toEqual([
      [6, uuid(100)],
      [12, uuid(101)],
    ]);
  });

  it('starts from the first row when fresh is set', async () => {
    await expect(importCsv({ ...base, sink: new CountingSink(new Map(), 3) })).rejects.toThrow();

    const writes = new Map<string, number>();
    const result = await importCsv({ ...base, fresh: true, sink: new CountingSink(writes) });

    expect(result).toMatchObject({ resumed: false, processed: 12, written: 10 });
    expect(writes.has(uuid(0))).toBe(true);
  });

  it('ignores a checkpoint that belongs to a different version of the file', async () => {
    await expect(importCsv({ ...base, sink: new CountingSink(new Map(), 3) })).rejects.toThrow();
    await writeFile(base.filePath, `${HEADER}\n${csvRow(uuid(0))}\n`);

    const result = await importCsv({ ...base, sink: new CountingSink(new Map()) });

    expect(result).toMatchObject({ resumed: false, processed: 1, written: 1 });
  });
});
