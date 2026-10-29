import { mkdtemp, readFile, rm, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { afterEach, beforeEach, describe, expect, it } from 'vitest';
import { importCsv } from '../src/import.ts';
import { TooManyInvalidRowsError } from '../src/pipeline/error-policy.ts';
import type { Transaction } from '../src/schema.ts';
import type { RowSink } from '../src/sinks/types.ts';

class MemorySink implements RowSink {
  batches: Transaction[][] = [];
  writeBatch(rows: Transaction[]) {
    this.batches.push(rows);
  }
  close() {}
}

const HEADER = 'id,user_email,amount,currency,status,created_at,description';
const row = (id: string, overrides: { amount?: string; description?: string } = {}) =>
  `${id},a@example.com,${overrides.amount ?? '1.00'},USD,completed,2026-01-02T03:04:05.000Z,${overrides.description ?? 'Coffee'}`;

const ids = [
  '00000000-0000-4000-8000-000000000001',
  '00000000-0000-4000-8000-000000000002',
  '00000000-0000-4000-8000-000000000003',
  '00000000-0000-4000-8000-000000000004',
];

let dir: string;
beforeEach(async () => {
  dir = await mkdtemp(join(tmpdir(), 'csv-import-'));
});
afterEach(async () => {
  await rm(dir, { recursive: true, force: true });
});

async function writeCsv(lines: string[]): Promise<string> {
  const path = join(dir, 'input.csv');
  // Leading BOM, as exported by Excel
  await writeFile(path, `\uFEFF${lines.join('\n')}\n`);
  return path;
}

describe('importCsv', () => {
  it('parses quoted commas, escaped quotes and multi-line fields in batches', async () => {
    const filePath = await writeCsv([
      HEADER,
      row(ids[0]!, { description: '"Groceries, weekly"' }),
      row(ids[1]!, { description: '"Book ""DDIA"""' }),
      row(ids[2]!, { description: '"Refund\nsee ticket"' }),
    ]);
    const sink = new MemorySink();

    const result = await importCsv({
      filePath,
      sink,
      rejectsPath: join(dir, 'rejects.ndjson'),
      batchSize: 2,
    });

    expect(result).toMatchObject({ processed: 3, invalid: 0, written: 3 });
    expect(sink.batches.map((b) => b.length)).toEqual([2, 1]);
    expect(sink.batches.flat().map((t) => t.description)).toEqual([
      'Groceries, weekly',
      'Book "DDIA"',
      'Refund\nsee ticket',
    ]);
  });

  // The two tests below depend on your handleInvalidRow implementation.
  it('writes invalid rows to the rejects file and keeps going', async () => {
    const filePath = await writeCsv([
      HEADER,
      row(ids[0]!),
      row(ids[1]!, { amount: '-5' }),
      row(ids[2]!),
    ]);
    const rejectsPath = join(dir, 'rejects.ndjson');

    const result = await importCsv({
      filePath,
      sink: new MemorySink(),
      rejectsPath,
      maxInvalidRows: 10,
    });

    expect(result).toMatchObject({ processed: 3, invalid: 1, written: 2 });
    const rejects = (await readFile(rejectsPath, 'utf8'))
      .trim()
      .split('\n')
      .map((l) => JSON.parse(l));
    expect(rejects).toHaveLength(1);
    expect(rejects[0]).toMatchObject({ line: 3, raw: { id: ids[1] } });
  });

  it('aborts the whole pipeline when too many rows are invalid', async () => {
    const filePath = await writeCsv([
      HEADER,
      // Valid row first: the error must surface even after rows were already yielded downstream.
      row(ids[0]!),
      row(ids[1]!, { amount: 'x' }),
      row(ids[2]!, { amount: 'y' }),
    ]);

    await expect(
      importCsv({
        filePath,
        sink: new MemorySink(),
        rejectsPath: join(dir, 'r.ndjson'),
        maxInvalidRows: 1,
      }),
    ).rejects.toBeInstanceOf(TooManyInvalidRowsError);
  });
});
