# csv-stream-processor

A learning project for importing multi-gigabyte CSV files (for example 2 GB, about 16 million rows) in Node.js with **constant memory**. It covers streams, backpressure, validation, batching, transactional database writes, invalid-row handling, and resuming after an interruption. Every design decision is backed by real measurements.

**How does it compare with popular libraries and Postgres's own loader, and why is it built this way?** See [docs/design-and-benchmarks.md](docs/design-and-benchmarks.md): a plain-language summary followed by benchmarks against `papaparse`, `csv-parser`, `fast-csv`, `psql \copy` and common `INSERT` approaches, plus one decision record per design choice.

## Contents

- [What you will learn](#what-you-will-learn)
- [Requirements](#requirements)
- [Quick start](#quick-start)
- [Commands](#commands)
- [Data format](#data-format)
- [Project layout](#project-layout)
- [Architecture](#architecture)
- [Sinks](#sinks)
- [Error handling](#error-handling)
- [Resuming after an interruption](#resuming-after-an-interruption)
- [Benchmarks](#benchmarks)
- [Testing](#testing)
- [Troubleshooting](#troubleshooting)
- [What production systems add](#what-production-systems-add)
- [Further exercises](#further-exercises)

## What you will learn

- Why `fs.readFile()` and `line.split(',')` break on large files, and how to replace them with streams and an RFC 4180 parser.
- How `stream/promises.pipeline()` connects stages, propagates backpressure, and cleans up on errors.
- Using **async generators** as pipeline stages instead of `class extends Transform`.
- Schema validation with zod, and storing money as integers to avoid floating-point errors.
- Batches and transactions, idempotent writes, and Postgres `COPY`.
- Why a UUID v4 primary key made inserts 7× slower, and how a staging table or UUID v7 fixes it.
- Checkpointing and resuming by byte offset: in a file after each commit (_at-least-once + idempotent_), or in the same database transaction as the batch (no replay).
- Two subtle traps hit while building this project (see [Error handling](#error-handling) and [Resuming](#resuming-after-an-interruption)).

## Requirements

| Tool    | Version | Notes                                                                                      |
| ------- | ------- | ------------------------------------------------------------------------------------------ |
| Node.js | ≥ 24    | Runs `.ts` files directly via type stripping; no `tsx` or build step needed. See `.nvmrc`. |
| pnpm    | 9       | Pinned in `packageManager`.                                                                |
| Docker  | any     | Only needed for Postgres.                                                                  |

Main stack: [`csv-parse`](https://csv.js.org/parse/) (streaming parser), [`zod`](https://zod.dev) (validation), `node:sqlite` (Node's built-in SQLite driver), and [`pg`](https://node-postgres.com) with [`pg-copy-streams`](https://github.com/brianc/node-pg-copy-streams) (Postgres `COPY`). Dev tooling: TypeScript 6, Vitest, ESLint (flat config), and Prettier.

## Quick start

```bash
pnpm install

# 1. Generate a 2 GB test file with ~0.1% invalid rows
pnpm generate --size-mb 2048 --id-format v7

# 2. Import into SQLite (data/import.db)
pnpm import data/transactions.csv --max-invalid 50000

# 3. Or import into Postgres
cp .env.example .env
pnpm db:up
pnpm import data/transactions.csv --sink postgres --max-invalid 50000
```

While importing, the CLI prints progress: percentage, rows/s, MB/s, invalid rows, heap, and RSS. You can watch memory stay flat no matter how big the file is.

Press **Ctrl+C** (or send `SIGTERM`, as `docker stop` and Kubernetes do) to stop safely, then run **the same command** again to continue from the last committed batch.

> The generated file has about 0.1% invalid rows. That is roughly 16,000 rows for a 2 GB file, far above the default `--max-invalid 1000`, which is why the examples raise the limit. Drop the flag to see the import abort on dirty data.

## Commands

### Scripts

| Command                | What it does                                                                               |
| ---------------------- | ------------------------------------------------------------------------------------------ |
| `pnpm generate`        | Generate a test CSV file                                                                   |
| `pnpm import`          | Import a CSV file                                                                          |
| `pnpm bench`           | Run the same file through several configurations and compare them                          |
| `pnpm compare:parsers` | Compare this repo's parser with `papaparse`, `csv-parser`, `fast-csv` and naive approaches |
| `pnpm db:up`           | Start Postgres 17 in Docker (port 5544)                                                    |
| `pnpm db:down`         | Stop Postgres                                                                              |
| `pnpm test`            | Run the tests (Vitest)                                                                     |
| `pnpm typecheck`       | `tsc --noEmit`                                                                             |
| `pnpm lint`            | ESLint                                                                                     |
| `pnpm format`          | Prettier                                                                                   |

### `pnpm generate`

| Option            | Default                 | Meaning                                                                                     |
| ----------------- | ----------------------- | ------------------------------------------------------------------------------------------- |
| `--out`           | `data/transactions.csv` | Output file                                                                                 |
| `--size-mb`       | `100`                   | Target size in MB                                                                           |
| `--invalid-ratio` | `0.001`                 | Share of deliberately invalid rows (bad email, negative amount, unknown currency, bad date) |
| `--id-format`     | `v4`                    | `v4` (random) or `v7` (time-ordered)                                                        |

The generated file deliberately contains the cases that break hand-written parsers: commas inside quotes, escaped `""` quotes, and line breaks inside a field.

### `pnpm import <file.csv>`

| Option           | Default                       | Meaning                                                                                                                                         |
| ---------------- | ----------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------- |
| `--sink`         | `sqlite`                      | `sqlite` or `postgres`                                                                                                                          |
| `--db`           | `data/import.db`              | SQLite file                                                                                                                                     |
| `--strategy`     | `direct`                      | SQLite write strategy: `direct` or `staging` (see [Sinks](#sinks))                                                                              |
| `--database-url` | `$DATABASE_URL`               | Postgres URL                                                                                                                                    |
| `--batch-size`   | `5000`                        | Rows per batch (one transaction each)                                                                                                           |
| `--max-invalid`  | `1000`                        | Invalid rows tolerated before the whole import is aborted                                                                                       |
| `--rejects`      | `data/rejects.ndjson`         | Where invalid rows are written                                                                                                                  |
| `--checkpoint`   | `data/import.checkpoint.json` | Checkpoint file used for resuming (SQLite only; Postgres keeps the checkpoint in the database, see [Resuming](#resuming-after-an-interruption)) |
| `--fresh`        | `false`                       | Ignore the checkpoint and start from the first row                                                                                              |

Exit codes:

| Code  | Meaning                                                                                         |
| ----- | ----------------------------------------------------------------------------------------------- |
| `0`   | Success                                                                                         |
| `1`   | Failure, including exceeding `--max-invalid` and a header mismatch                              |
| `2`   | Invalid arguments                                                                               |
| `75`  | The same file is already being imported into the same Postgres table (`EX_TEMPFAIL`: try later) |
| `130` | Stopped with Ctrl+C (`SIGINT`)                                                                  |
| `143` | Stopped with `SIGTERM`                                                                          |

On `SIGINT` or `SIGTERM` the import stops reading, keeps every committed batch, and exits. If it has not finished within 10 seconds (for example a query is stuck), it prints a message and exits anyway with the same code; 10 s stays below Kubernetes' default 30 s `terminationGracePeriodSeconds`. While it is stopping, a second, different signal is ignored; sending the same signal again kills the process immediately with Node's default behaviour.

These are the exit codes of `node src/cli.ts …`. Under `pnpm import`, pnpm receives the signal as well and may exit with its own code (see [Troubleshooting](#troubleshooting)). In a container, use the command behind `pnpm import` as the entrypoint, so the orchestrator sees 130, 143 and 75:

```bash
node --env-file-if-exists=.env --disable-warning=ExperimentalWarning src/cli.ts data/transactions.csv --sink postgres
```

### `pnpm bench <file.csv>`

| Option          | Default                        | Meaning                                              |
| --------------- | ------------------------------ | ---------------------------------------------------- |
| `--targets`     | `sqlite-direct,sqlite-staging` | Any of `sqlite-direct`, `sqlite-staging`, `postgres` |
| `--batch-sizes` | `5000`                         | Comma-separated batch sizes, e.g. `100,1000,5000`    |

Every run starts from an empty destination. For Postgres, the benchmark uses its own `bench_transactions` table, so it never touches the real `transactions` table.

## Data format

### Input CSV

The file has a header row and may start with a BOM (as Excel exports do). The header must contain exactly the columns below, in any order. On a fresh import a missing, unexpected or duplicated column aborts with `HeaderMismatchError` before any row is read; without this check a misnamed column would fail every row, and an extra one would be silently dropped by zod. A resumed import reuses the header saved in the checkpoint.

| Column        | Validation rule                                                | Stored as                                           |
| ------------- | -------------------------------------------------------------- | --------------------------------------------------- |
| `id`          | UUID                                                           | primary key                                         |
| `user_email`  | valid email                                                    |                                                     |
| `amount`      | positive, at most 12 integer digits and 2 decimal places       | `amount_minor`: integer cents, e.g. `12.5` → `1250` |
| `currency`    | `USD`, `EUR`, `GBP`, `SGD`                                     |                                                     |
| `status`      | `pending`, `completed`, `failed`                               |                                                     |
| `created_at`  | ISO 8601 UTC, e.g. `2026-01-02T03:04:05.000Z`                  |                                                     |
| `description` | at most 500 characters; commas, quotes and line breaks allowed |                                                     |

Amounts are converted to integers by splitting the string, not by multiplying floats, because `Number('0.29') * 100 === 28.999999999999996`.

### `rejects.ndjson`

One JSON object per line. `line` is the physical line in the file where the record ends; a record can span several lines when a field contains line breaks.

```text
{"line":2773,"reason":"user_email: Invalid email address","raw":{"id":"bffdf3f7-…","user_email":"not-an-email","amount":"5042.82","currency":"GBP","status":"failed","created_at":"2026-04-12T18:28:48.840Z","description":"Coffee"}}
```

Fix these rows and import them separately; the sinks are idempotent, so duplicates are not a concern.

### Destination table

The `transactions` table has `id` (PK), `user_email`, `amount_minor`, `currency`, `status`, `created_at`, and `description`. On Postgres the columns use `uuid`, `bigint`, `char(3)` and `timestamptz`.

## Project layout

```
src/
  cli.ts                 # parse arguments, print progress, handle Ctrl+C
  import.ts              # importCsv(): build the pipeline, checkpoint, resume
  schema.ts              # zod schema: strings → typed data
  checkpoint.ts          # load / save (atomic) / clear the checkpoint file
  retry.ts               # withRetry(): exponential backoff with full jitter
  csv.ts                 # RFC 4180 CSV escaping
  uuid.ts                # UUIDv7 (RFC 9562)
  pipeline/
    header.ts            # checkHeader(): reject a header with missing / unexpected / duplicate columns
    validate.ts          # validateRows(): validate + track the byte offset
    batch.ts             # batch(): group rows into arrays
    error-policy.ts      # handleInvalidRow(): decide what happens to invalid rows
  sinks/
    types.ts             # RowSink interface (optional lock and checkpoint store)
    sqlite.ts            # direct / staging strategies
    postgres.ts          # COPY into the table; on duplicate ids, merge via temp table; checkpoint + lock
    index.ts             # createSink() from configuration
scripts/
  generate-csv.ts        # test data generator
  bench.ts               # multi-configuration benchmark
  compare-parsers.ts     # this repo vs other CSV libraries
  compare-loaders.ts     # this repo vs common Postgres loading approaches
docs/
  design-and-benchmarks.md  # comparisons and decision records
test/                    # Vitest: unit, integration, resume, Postgres
.github/workflows/ci.yml # lint, typecheck, format check and tests against Postgres 17
compose.yaml             # Postgres 17 for development
```

## Architecture

```
createReadStream (1 MiB chunks, start = checkpoint.bytes)
  → csv-parse          bytes → records (BOM, "quoted, comma", "", line breaks inside fields)
  → validateRows       zod: strings → typed data; invalid rows → handleInvalidRow → rejects.ndjson
  → batch(5000)        group rows into arrays
  → sink.writeBatch    one transaction per batch, idempotent
                       Postgres: the checkpoint is upserted in the same transaction
  → saveCheckpoint     SQLite only: byte offset + line count + counters, written atomically (tmp file → rename)
```

Everything is wired together with `pipeline()` from `node:stream/promises`. When any stage throws, or on Ctrl+C / `SIGTERM`, every stage is torn down and file descriptors are closed.

| Problem                       | Solution                                                                                                  | File                                                         |
| ----------------------------- | --------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------ |
| File larger than RAM          | Read in 1 MiB chunks, never `readFile`                                                                    | [src/import.ts](src/import.ts)                               |
| Producer faster than consumer | Backpressure: stages are _pull-based_ async generators, so the file is only read when the sink is ready   | [src/pipeline/batch.ts](src/pipeline/batch.ts)               |
| CSV is not just `split(',')`  | `csv-parse` follows RFC 4180; `max_record_size` (64 KB) stops one unclosed quote from swallowing the file | [src/import.ts](src/import.ts)                               |
| Dirty data                    | The zod schema is the single place where text becomes typed data                                          | [src/schema.ts](src/schema.ts)                               |
| Invalid rows                  | Written to `rejects.ndjson` with line number and reason; the import aborts past a limit                   | [src/pipeline/error-policy.ts](src/pipeline/error-policy.ts) |
| Slow database writes          | Batches + transactions; SQLite uses prepared statements or staging; Postgres uses `COPY`                  | [src/sinks/](src/sinks/)                                     |
| Crash halfway through         | Checkpoint with every batch (Postgres: same transaction; SQLite: file after commit) + idempotent sinks    | [src/import.ts](src/import.ts)                               |
| Ctrl+C, `SIGTERM`             | `AbortController` → `pipeline({ signal })`, forced exit after 10 s                                        | [src/cli.ts](src/cli.ts)                                     |

Sink lifecycle: `lock` (optional) → `open` → `writeBatch` (many times) → `finish` (only when the whole file was imported) → `close`. Each `writeBatch` is awaited before the next batch is pulled, so a slow sink slows down file reading instead of filling up memory.

## Sinks

| Sink               | How it writes                                                                                                                                                                       | When to use                            |
| ------------------ | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | -------------------------------------- |
| **SQLite direct**  | `INSERT ... ON CONFLICT DO NOTHING` straight into the table with the primary key                                                                                                    | Ordered keys (UUID v7, auto-increment) |
| **SQLite staging** | Append to an index-free table; `finish()` merges everything once with `INSERT ... SELECT ... ORDER BY id`                                                                           | Random keys (UUID v4)                  |
| **Postgres**       | Per batch: `COPY` straight into the table inside a savepoint; only if an id already exists, roll back to the savepoint and merge through a temp table with `ON CONFLICT DO NOTHING` | Production                             |

- `COPY` is the fastest way to bulk-load Postgres, but it cannot skip duplicates. Duplicates only appear when a batch is replayed after a crash or a file is re-imported, so the common case takes the plain `COPY` path. Always merging through the temp table was 2.5× slower ([details](docs/design-and-benchmarks.md#a-bug-the-benchmark-found)); `sink.stats` reports how many batches took each path.
- When escaping CSV for `COPY`, empty strings must be quoted (`""`), because `COPY ... (FORMAT csv)` reads an unquoted empty field as `NULL`.
- Staging: on resume the staging table is kept; on a fresh run it is cleared, so leftovers from an abandoned run are never merged by mistake.
- **One import per file and table.** Before reading the checkpoint, the Postgres sink takes a session-level advisory lock keyed on `<table>:<absolute file path>`. A second import of the same file into the same table fails at once with `ImportInProgressError` (exit code 75). The lock is released when the import closes its connection, or by Postgres when the process dies. The lock exists only for Postgres: two concurrent SQLite imports of the same file would share the checkpoint and rejects files, so don't run them at the same time.
- **Retries.** A batch that Postgres rolls back with `40001` (serialization failure) or `40P01` (deadlock) is retried, up to 3 attempts in total, with exponential backoff and full jitter. A lost connection is not retried in the same process, because the lock and the temp table die with the session: the import fails, and running it again resumes from the checkpoint. If `ROLLBACK` itself fails, the original error is reported (the rollback error is attached as `rollbackError`).
- **Schema.** The sink creates `transactions` and `import_checkpoints` itself with `CREATE TABLE IF NOT EXISTS`, behind a global transaction-level advisory lock, because `IF NOT EXISTS` can still fail when two sessions create the same table at once. The database user therefore needs `CREATE` privilege on the schema, and `TEMPORARY` privilege on the database for the per-session `staging` temp table (granted to `PUBLIC` by default).
- The sink keeps a no-op `'error'` listener on its client, so a connection the server drops between queries makes the next query fail instead of crashing the process.

## Error handling

**Invalid rows** go through [`handleInvalidRow`](src/pipeline/error-policy.ts). It increments the counter, writes the row to the rejects file, and throws `TooManyInvalidRowsError` once `--max-invalid` is exceeded. The row is written **before** throwing, so the very row that crossed the limit is still in `rejects.ndjson`. `--max-invalid 0` means fail on the first invalid row, which suits financial data.

**Trap 1: the real error is masked by `AbortError`.** When an async generator throws inside `for await (... of parserStream)`, the default iterator destroys the csv-parse stream with a generic `AbortError`, and that error reaches `pipeline()` before the original one. You get "The operation was aborted" instead of `TooManyInvalidRowsError`. It only happens after at least one valid row has passed through, so it easily slips past tests.

The fix:

```ts
(source: Parser) => validateRows(source.iterator({ destroyOnReturn: false }), ...)
```

`pipeline()` still destroys the parser, but with the original error. A regression test lives in [test/import.test.ts](test/import.test.ts).

## Resuming after an interruption

With every batch the importer records a checkpoint: the byte offset, line count, header, counters, and the size of the rejects file. Where it is stored depends on the sink:

- **Postgres** stores it as `jsonb` in the `import_checkpoints` table, keyed on `<table>:<absolute file path>`, and upserts it in the **same transaction** as the batch. The rows and the checkpoint commit together or not at all, so a resumed import never writes a batch twice. `--checkpoint` is ignored. The row is deleted once the whole file has been imported.
- **SQLite** writes it to the `--checkpoint` file after the batch commits ([src/checkpoint.ts](src/checkpoint.ts)), via a temp file and a rename, so a crash never leaves a corrupt checkpoint behind.

**Upgrading:** a Postgres import interrupted with an older version, which only wrote the checkpoint file, restarts from the first row with this version. That is safe, because the sink skips rows it already has, but it redoes the work.

How resume stays correct:

- **Exact offsets.** csv-parse's `info.bytes_records` is the absolute byte offset just after the record it emitted (including the BOM). The downstream stages are pull-based generators, so when a batch reaches the sink, `validateRows` is paused exactly at the batch's last row. The offset therefore points at the end of the batch, not at rows csv-parse has already buffered ahead.
- **SQLite: at-least-once + idempotent = effectively-once.** The checkpoint file is written _after_ the commit. If a crash happens between the two, the next run writes one batch again and `ON CONFLICT DO NOTHING` skips the duplicates. With Postgres this window does not exist.
- **Only resume when it is safe.** The file's path, size, and mtime and the destination must all match. If the file changed, the old offset is meaningless, so the checkpoint is ignored.
- **The header is saved**, because a resumed read starts mid-file, after the header line.
- **Counters are restored**, so `--max-invalid` applies to the _whole file_, not to each run.
- **Trap 2: duplicate rejects.** Invalid rows after the checkpoint were already written by the previous run, and are read and rejected again on resume. The database is fine thanks to idempotency, but the rejects file is not. The fix: store the rejects file's size in the checkpoint and truncate the file back to it on resume. Invalid rows are written synchronously (`writeSync`); they are rare, and this keeps the size on disk in step with the checkpoint. [test/resume.test.ts](test/resume.test.ts) caught this bug.

`written` counts rows handed to the sink, not rows actually inserted: when a batch takes the merge path, rows whose id already exists are skipped but still counted. Compare with `rowsInDb`.

Verified end-to-end against Postgres on a 2.45-million-row file: Ctrl+C at 700,000 rows (the database and the checkpoint agree) → run again → all 2,453,500 valid rows present, 2,453,500 distinct ids, and 2,500 rejects with no duplicate lines.

## Benchmarks

300 MB file, 2,456,000 rows (0.1% invalid), MacBook, Postgres 17 in Docker, measured with `pnpm bench`. Treat the numbers as relative: other machines differ, and Docker's disk speed drifted by ~2.5× between sessions, so only numbers from the same session are compared. The Postgres numbers come from a later session than the SQLite ones (after the sink fix); `psql \copy` took 36.2 s in that session. Comparisons with other libraries and loaders are in [docs/design-and-benchmarks.md](docs/design-and-benchmarks.md).

### Write strategy × ID format (batch size 5000)

| Sink           | UUID v4 (random)    | UUID v7 (time-ordered) |
| -------------- | ------------------- | ---------------------- |
| SQLite direct  | 122 s (20k rows/s)  | **17 s** (144k rows/s) |
| SQLite staging | 21 s (115k rows/s)  | —                      |
| Postgres COPY  | 37.4 s (66k rows/s) | 30.9 s (80k rows/s)    |

Peak heap stayed at 70–120 MB in every configuration, regardless of file size.

- **The UUID v4 primary key is the culprit.** Each insert lands on a random B-tree page; once the index outgrows the page cache, almost every insert hits the disk. With SQLite's default cache (~2 MB) the same file took ~344 s; raising `cache_size` to 64 MB brought it to 122 s.
- **Staging** turns random inserts into one sorted merge, so the B-tree is written in key order.
- **UUID v7** gets the same effect without a merge step: new keys are always larger than old ones, so inserts only append to the B-tree's rightmost page. Node 24 has no `crypto.randomUUIDv7()` yet; see [src/uuid.ts](src/uuid.ts).
- **Postgres gains less from v7 (~17%)** than SQLite at this scale: the uuid index for 2.4 million rows (an estimated ~75 MB) still fits in the default 128 MB `shared_buffers`. The gap should grow once the index outgrows the buffer cache.
- Without an index, the read-parse-validate pipeline handles 300 MB in about 17 s. The bottleneck is the database, not the stream.

### Checkpoint in the transaction (Postgres)

The Postgres sink now upserts its checkpoint in each batch transaction: one small extra statement per batch, about 490 for this file. `pnpm bench` does not pass a checkpoint, so the tables above never exercised it. It was measured with the CLI instead (`pnpm import … --sink postgres --fresh`), as an interleaved A/B against `master` in a later session. No large regression was detected, but the machine was heavily loaded (load average 14–30, runs from 34 s to 185 s), so a small difference cannot be ruled out; it should be re-measured on an idle machine. Details in [docs/design-and-benchmarks.md](docs/design-and-benchmarks.md#14-checkpoint-inside-the-database-transaction).

### Batch size (UUID v4)

| Batch size | Postgres COPY        | SQLite staging     |
| ---------- | -------------------- | ------------------ |
| 100        | 136.5 s (18k rows/s) | 21 s (118k rows/s) |
| 1000       | 52.3 s (47k rows/s)  | 21 s (118k rows/s) |
| 5000       | 37.4 s (66k rows/s)  | 21 s (116k rows/s) |
| 20000      | 44.2 s (56k rows/s)  | 22 s (110k rows/s) |

- **Postgres shows a U-shaped curve.** With small batches, every 100 rows pay four network round-trips (`BEGIN`, `SAVEPOINT`, `COPY`, `COMMIT`). With very large batches, peak heap grows (107–112 MB up to 5000, 145 MB at 20,000) and each bigger transaction gets slower. The sweet spot is around 5000.
- **SQLite barely cares about batch size**: it runs in-process with no network, and with WAL + `synchronous = NORMAL` each commit is cheap.
- There is no universally "right" batch size. It depends on each sink's fixed cost per write, so measure instead of guessing.

## Testing

```bash
pnpm test           # run once
pnpm test:watch     # watch mode
```

| File                                                   | What it checks                                                                                                                                                                                                                                                      |
| ------------------------------------------------------ | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| [test/import.test.ts](test/import.test.ts)             | Tricky CSV (quotes, commas, line breaks, BOM), batching, rejects, aborting past the limit, a wrong header rejected before any row is read                                                                                                                           |
| [test/resume.test.ts](test/resume.test.ts)             | Crash → resume: every row written exactly once, absolute reject line numbers, no replay when the sink commits the checkpoint, `--fresh`, changed file                                                                                                               |
| [test/sqlite.test.ts](test/sqlite.test.ts)             | direct / staging, duplicate skipping, keeping or clearing staging                                                                                                                                                                                                   |
| [test/postgres.test.ts](test/postgres.test.ts)         | `COPY` fast path and duplicate fallback, duplicates inside one batch, empty strings not becoming `NULL`, checkpoint in the batch transaction, concurrent table creation, the import lock, a dropped connection, retry on serialization failure, invalid table names |
| [test/header.test.ts](test/header.test.ts)             | Missing, unexpected and duplicate header columns                                                                                                                                                                                                                    |
| [test/retry.test.ts](test/retry.test.ts)               | Backoff: retries only retryable errors, gives up after the last attempt                                                                                                                                                                                             |
| [test/error-policy.test.ts](test/error-policy.test.ts) | Invalid-row limit, failing fast when `maxInvalidRows = 0`                                                                                                                                                                                                           |
| `schema`, `batch`, `csv`, `uuid`                       | Unit tests                                                                                                                                                                                                                                                          |

The Postgres tests only run when `DATABASE_URL` is set; it is loaded from `.env` by [vitest.config.ts](vitest.config.ts). Otherwise they are skipped locally. In CI (`CI` set) a missing `DATABASE_URL` is an error, so a broken setup cannot pass by skipping. Each run uses its own throwaway table and drops it afterwards.

[CI](.github/workflows/ci.yml) runs on pushes to `master`/`main` and on pull requests: `pnpm lint`, `pnpm typecheck`, `prettier --check .`, and `pnpm test` against a Postgres 17 service container.

## Troubleshooting

| Symptom                                                             | Cause and fix                                                                                                                   |
| ------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------- |
| `TooManyInvalidRowsError` on resume although the first run was fine | Counters are restored, so the limit applies to the whole file. Raise `--max-invalid`.                                           |
| `ExperimentalWarning: SQLite is an experimental feature`            | `node:sqlite` is still experimental in Node 24. The `pnpm import` script silences it.                                           |
| An interrupted import does not resume                               | The checkpoint is only used when the file and destination match. Changing the file or `--sink`/`--db`/`--strategy` starts over. |
| Ctrl+C under `pnpm import` exits with code 1                        | pnpm receives the signal too. Run `node src/cli.ts <file>` to see the real exit code 130.                                       |
| Exit code 75: `another import of … is already running`              | Another process is importing the same file into the same table. Wait for it, or stop it; the lock is released when it exits.    |
| `HeaderMismatchError`                                               | The header has missing, unexpected or duplicated columns. Fix the header; see [Input CSV](#input-csv).                          |
| Port 5544 is already in use                                         | Change `ports` in [compose.yaml](compose.yaml) and `DATABASE_URL` in `.env`.                                                    |
| SQLite inserts are very slow with UUID v4                           | Expected, see [Benchmarks](#benchmarks): use `--strategy staging` or UUID v7.                                                   |

TypeScript is pinned to 6.0 because `typescript-eslint` does not support TypeScript 7 yet.

## What production systems add

- **Uploads**: clients upload directly to S3/GCS (presigned URLs, multipart). The API only receives the file location and enqueues a job (BullMQ, SQS). Never import inside an HTTP request.
- **Migrations**: the importer creates its own tables, which needs `CREATE` privilege. Production schemas are usually owned by a migration tool, with the importer running as a less privileged user.
- **Duplicate-id policy**: a row whose id already exists is skipped (`ON CONFLICT DO NOTHING`). Whether it should be skipped, updated, or reported is a business decision.
- **Concurrent workers**: the Postgres lock allows one import per file and table, but if a worker loses its connection mid-import it can still append rejects for up to one batch before its next write fails. Workers must not share a rejects path.
- **CPU-bound work**: if validation is heavy, spread it across `worker_threads`.
- **Observability**: structured logs (pino) and metrics: rows/s, invalid rows, time per batch.

## Further exercises

1. Give Postgres a table that already holds 20 million rows and re-measure v4 vs v7. Does the gap appear?
2. The Postgres sink already stores the checkpoint in the batch transaction. Drop `ON CONFLICT` there: is resume still correct, and what breaks when the same file is imported twice?
3. Move validation into `worker_threads` and measure whether the bottleneck moves.
4. Upload the file to S3 (LocalStack or MinIO) and import straight from the `GetObject` stream.
