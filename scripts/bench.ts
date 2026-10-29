/**
 * Imports the same CSV through several sink configurations and prints a Markdown table.
 *
 *   pnpm bench data/transactions.csv --targets sqlite-direct,sqlite-staging,postgres --batch-sizes 1000,5000
 *
 * Every run starts from an empty destination. Postgres uses its own `bench_transactions`
 * table so benchmarking never touches the real `transactions` table.
 */
import { mkdir, rm } from 'node:fs/promises';
import { parseArgs } from 'node:util';
import pg from 'pg';
import { importCsv } from '../src/import.ts';
import { PostgresSink } from '../src/sinks/postgres.ts';
import { SqliteSink } from '../src/sinks/sqlite.ts';
import type { RowSink } from '../src/sinks/types.ts';

const TARGETS = ['sqlite-direct', 'sqlite-staging', 'postgres'] as const;
type Target = (typeof TARGETS)[number];

const BENCH_DIR = 'data/bench';
const SQLITE_PATH = `${BENCH_DIR}/bench.db`;
const PG_TABLE = 'bench_transactions';

const { values, positionals } = parseArgs({
  allowPositionals: true,
  options: {
    targets: { type: 'string', default: 'sqlite-direct,sqlite-staging' },
    'batch-sizes': { type: 'string', default: '5000' },
  },
});

const filePath = positionals[0];
if (!filePath) throw new Error('Usage: pnpm bench <file.csv> [--targets ...] [--batch-sizes ...]');

const targets = values.targets.split(',').map((t) => {
  if (!(TARGETS as readonly string[]).includes(t)) throw new Error(`unknown target ${t}`);
  return t as Target;
});
const batchSizes = values['batch-sizes'].split(',').map(Number);
const databaseUrl = process.env.DATABASE_URL;
if (targets.includes('postgres') && !databaseUrl)
  throw new Error('postgres target needs DATABASE_URL');

async function freshSink(target: Target): Promise<RowSink> {
  if (target === 'postgres') {
    const admin = new pg.Client({ connectionString: databaseUrl });
    await admin.connect();
    await admin.query(`DROP TABLE IF EXISTS ${PG_TABLE}`);
    await admin.end();
    return new PostgresSink(databaseUrl!, { table: PG_TABLE });
  }
  await Promise.all(['', '-wal', '-shm'].map((s) => rm(`${SQLITE_PATH}${s}`, { force: true })));
  return new SqliteSink(SQLITE_PATH, {
    strategy: target === 'sqlite-staging' ? 'staging' : 'direct',
  });
}

await mkdir(BENCH_DIR, { recursive: true });
const results: string[] = [];

for (const target of targets) {
  for (const batchSize of batchSizes) {
    const sink = await freshSink(target);
    let peakHeap = 0;
    try {
      const result = await importCsv({
        filePath,
        sink,
        rejectsPath: `${BENCH_DIR}/rejects.ndjson`,
        batchSize,
        maxInvalidRows: Number.POSITIVE_INFINITY,
        progressIntervalMs: 250,
        onProgress: () => (peakHeap = Math.max(peakHeap, process.memoryUsage().heapUsed)),
      });
      const seconds = result.durationMs / 1000;
      const line =
        `| ${target} | ${batchSize} | ${seconds.toFixed(1)} | ` +
        `${Math.round(result.processed / seconds).toLocaleString()} | ` +
        `${(peakHeap / 1024 / 1024).toFixed(0)} | ${(await sink.count?.())?.toLocaleString()} |`;
      results.push(line);
      console.error(line);
    } finally {
      await sink.close();
    }
  }
}

console.log('\n| target | batch size | seconds | rows/s | peak heap MB | rows in db |');
console.log('| --- | --- | --- | --- | --- | --- |');
console.log(results.join('\n'));
