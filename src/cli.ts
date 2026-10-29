import { parseArgs } from 'node:util';
import { importCsv, type ImportProgress } from './import.ts';
import { createSink, type SinkConfig } from './sinks/index.ts';
import { ImportInProgressError } from './sinks/types.ts';

const USAGE = `Usage: pnpm import <file.csv> [options]

  --sink sqlite|postgres     destination (default: sqlite)
  --db <path>                SQLite file (default: data/import.db)
  --strategy direct|staging  SQLite write strategy (default: direct)
  --database-url <url>       Postgres URL (default: $DATABASE_URL)
  --batch-size <n>           rows per batch/transaction (default: 5000)
  --max-invalid <n>          invalid rows tolerated before aborting (default: 1000)
  --rejects <path>           invalid rows as NDJSON (default: data/rejects.ndjson)
  --checkpoint <path>        resume file (default: data/import.checkpoint.json)
  --fresh                    ignore the checkpoint and start from the first row`;

const { values, positionals } = parseArgs({
  allowPositionals: true,
  options: {
    sink: { type: 'string', default: 'sqlite' },
    db: { type: 'string', default: 'data/import.db' },
    strategy: { type: 'string', default: 'direct' },
    'database-url': { type: 'string', default: process.env.DATABASE_URL },
    'batch-size': { type: 'string', default: '5000' },
    'max-invalid': { type: 'string', default: '1000' },
    rejects: { type: 'string', default: 'data/rejects.ndjson' },
    checkpoint: { type: 'string', default: 'data/import.checkpoint.json' },
    fresh: { type: 'boolean', default: false },
  },
});

function fail(message: string): never {
  console.error(`${message}\n\n${USAGE}`);
  process.exit(2);
}

const filePath = positionals[0] ?? fail('Missing <file.csv>.');

function sinkConfig(): SinkConfig {
  if (values.sink === 'postgres') {
    return {
      kind: 'postgres',
      databaseUrl:
        values['database-url'] || fail('--sink postgres needs --database-url or $DATABASE_URL.'),
    };
  }
  if (values.sink !== 'sqlite') fail(`Unknown --sink ${values.sink}.`);
  if (values.strategy !== 'direct' && values.strategy !== 'staging') {
    fail(`Unknown --strategy ${values.strategy}.`);
  }
  return { kind: 'sqlite', path: values.db, strategy: values.strategy };
}

const mb = (bytes: number) => (bytes / 1024 / 1024).toFixed(0);

function printProgress(p: ImportProgress): void {
  const seconds = p.elapsedMs / 1000;
  const percent = ((p.bytesRead / p.totalBytes) * 100).toFixed(1);
  const rowsPerSec = Math.round((p.processed - p.startedAt.processed) / seconds).toLocaleString();
  const mbPerSec = ((p.bytesRead - p.startedAt.bytes) / 1024 / 1024 / seconds).toFixed(1);
  const { heapUsed, rss } = process.memoryUsage();
  process.stderr.write(
    `\r${percent.padStart(5)}%  ${p.processed.toLocaleString()} rows  ${rowsPerSec} rows/s  ` +
      `${mbPerSec} MB/s  invalid=${p.invalid}  heap=${mb(heapUsed)}MB rss=${mb(rss)}MB   `,
  );
}

// Ctrl+C (SIGINT) and container stops (SIGTERM) stop reading cleanly: pipeline() tears down
// every stage and the sink keeps only fully committed batches; the next run resumes from there.
const SHUTDOWN_GRACE_MS = 30_000;
const controller = new AbortController();
let abortExitCode = 130;
for (const [signal, exitCode] of [
  ['SIGINT', 130],
  ['SIGTERM', 143],
] as const) {
  process.once(signal, () => {
    process.stderr.write(`\n${signal} received, stopping import...\n`);
    abortExitCode = exitCode;
    controller.abort();
    // The in-flight batch normally commits within a second; never hang a stop on a stuck query.
    setTimeout(() => process.exit(exitCode), SHUTDOWN_GRACE_MS).unref();
  });
}

const { sink, target } = createSink(sinkConfig());
try {
  const result = await importCsv({
    filePath,
    sink,
    rejectsPath: values.rejects,
    batchSize: Number(values['batch-size']),
    maxInvalidRows: Number(values['max-invalid']),
    checkpoint: { path: values.checkpoint, target },
    fresh: values.fresh,
    signal: controller.signal,
    onProgress: printProgress,
  });
  process.stderr.write('\n');
  console.log({
    ...result,
    durationSec: +(result.durationMs / 1000).toFixed(1),
    rowsInDb: await sink.count?.(),
  });
} catch (error) {
  process.stderr.write('\n');
  if (error instanceof Error && error.name === 'AbortError') {
    console.error('Import aborted. Committed batches are kept; run the same command to resume.');
    process.exitCode = abortExitCode;
  } else if (error instanceof ImportInProgressError) {
    console.error(error.message);
    process.exitCode = 75; // EX_TEMPFAIL: try again later
  } else {
    console.error(error);
    process.exitCode = 1;
  }
} finally {
  await sink.close();
}
