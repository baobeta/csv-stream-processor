/**
 * Loads a CSV into Postgres with one approach and prints JSON with rows, seconds and peak RSS.
 * Run one approach per process so peak memory is measured independently:
 *
 *   node --env-file=.env scripts/compare-loaders.ts <approach> <file.csv> [maxRows]
 *
 * Approaches mirror what tutorials and projects commonly do, from naive to this repo:
 *   row-insert      parse with csv-parse, one INSERT per row (autocommit), no validation
 *   batched-insert  parse with csv-parse, multi-row INSERT of 1000 rows, no validation
 *   this-repo       importCsv() + PostgresSink: validation, rejects, COPY, checkpoints
 *
 * The native `psql \copy` baseline is run from the shell; see docs/design-and-benchmarks.md.
 */
import { createReadStream } from 'node:fs';
import { rm } from 'node:fs/promises';
import { parse } from 'csv-parse';
import pg from 'pg';
import { importCsv } from '../src/import.ts';
import { PostgresSink } from '../src/sinks/postgres.ts';

const [approach, file, maxRowsArg] = process.argv.slice(2);
const databaseUrl = process.env.DATABASE_URL;
if (!approach || !file || !databaseUrl) {
  throw new Error('Usage: compare-loaders.ts <approach> <file.csv> [maxRows] (needs DATABASE_URL)');
}
const maxRows = maxRowsArg ? Number(maxRowsArg) : Number.POSITIVE_INFINITY;

const TABLE = 'compare_transactions';
const COLUMNS = ['id', 'user_email', 'amount', 'currency', 'status', 'created_at', 'description'];

async function freshTable(client: pg.Client): Promise<void> {
  await client.query(`DROP TABLE IF EXISTS ${TABLE}`);
  await client.query(`
    CREATE TABLE ${TABLE} (
      id uuid PRIMARY KEY, user_email text NOT NULL, amount numeric(14, 2) NOT NULL,
      currency char(3) NOT NULL, status text NOT NULL, created_at timestamptz NOT NULL,
      description text NOT NULL
    )`);
}

async function* records(): AsyncGenerator<string[]> {
  let n = 0;
  for await (const record of createReadStream(file!).pipe(parse({ from_line: 2 }))) {
    if (n++ >= maxRows) return;
    yield record as string[];
  }
}

async function rowInsert(client: pg.Client): Promise<number> {
  let rows = 0;
  const sql = `INSERT INTO ${TABLE} (${COLUMNS.join(', ')}) VALUES ($1, $2, $3, $4, $5, $6, $7)`;
  for await (const record of records()) {
    await client.query(sql, record);
    rows++;
  }
  return rows;
}

async function batchedInsert(client: pg.Client): Promise<number> {
  let rows = 0;
  let buffer: string[][] = [];
  const flush = async () => {
    if (buffer.length === 0) return;
    const placeholders = buffer.map(
      (_, r) => `(${COLUMNS.map((__, c) => `$${r * COLUMNS.length + c + 1}`).join(', ')})`,
    );
    await client.query(
      `INSERT INTO ${TABLE} (${COLUMNS.join(', ')}) VALUES ${placeholders.join(', ')}`,
      buffer.flat(),
    );
    rows += buffer.length;
    buffer = [];
  };
  for await (const record of records()) {
    buffer.push(record);
    if (buffer.length === 1000) await flush();
  }
  await flush();
  return rows;
}

async function thisRepo(): Promise<number> {
  const checkpoint = 'data/compare.checkpoint.json';
  await rm(checkpoint, { force: true });
  const sink = new PostgresSink(databaseUrl!, { table: 'compare_repo_transactions' });
  const admin = new pg.Client({ connectionString: databaseUrl });
  await admin.connect();
  await admin.query('DROP TABLE IF EXISTS compare_repo_transactions');
  await admin.end();
  try {
    const result = await importCsv({
      filePath: file!,
      sink,
      rejectsPath: 'data/compare-rejects.ndjson',
      maxInvalidRows: Number.POSITIVE_INFINITY,
      checkpoint: { path: checkpoint, target: 'compare' },
    });
    return result.written;
  } finally {
    await sink.close();
  }
}

const client = new pg.Client({ connectionString: databaseUrl });
await client.connect();
const startedAt = performance.now();
try {
  let rows: number;
  if (approach === 'this-repo') {
    rows = await thisRepo();
  } else {
    await freshTable(client);
    rows = approach === 'row-insert' ? await rowInsert(client) : await batchedInsert(client);
  }
  console.log(
    JSON.stringify({
      ok: true,
      approach,
      rows,
      seconds: (performance.now() - startedAt) / 1000,
      peakRssMb: process.resourceUsage().maxRSS / 1024,
    }),
  );
} catch (error) {
  console.log(
    JSON.stringify({
      ok: false,
      approach,
      seconds: (performance.now() - startedAt) / 1000,
      error: (error as Error).message.slice(0, 160),
    }),
  );
} finally {
  await client.end();
}
