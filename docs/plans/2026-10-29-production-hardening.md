# Production Hardening Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Make the importer safe to run unattended against a real Postgres. Wrong files fail at the header. Batches are never replayed after a crash. Two workers can never import the same file at once. Transient database conflicts are retried. Container stops (SIGTERM) shut down cleanly. CI runs every test, including the Postgres tests.

**Architecture:** The checkpoint moves from a local JSON file into a Postgres table (`import_checkpoints`) and is written in the **same transaction** as each batch. Rows and progress therefore commit together or not at all. A session-level advisory lock, keyed by target table and file path, is taken before the checkpoint is read. The file-based checkpoint stays as the fallback for sinks with no store of their own (SQLite, test sinks). Out of scope for this plan: S3 and object storage, job queues, metrics, migration tooling, and the duplicate-id policy (see the end of this plan).

**Tech Stack:** Node 24 (native TypeScript type stripping, so `.ts` imports, no parameter properties, no enums), pnpm 9, Vitest, `pg` + `pg-copy-streams`, `csv-parse` v7, zod 4, Postgres 17 (Docker, port 5544), GitHub Actions.

---

## Before you start: things you must know about this repo

- **Run TypeScript directly.** `node src/cli.ts` works because Node 24 strips types. Write imports with the `.ts` extension. Don't use constructor parameter properties (`constructor(private x)`), because `erasableSyntaxOnly` rejects them. Declare fields explicitly instead, as in `test/resume.test.ts` `CountingSink`.
- **Postgres tests skip themselves** unless `DATABASE_URL` is set. Locally: run `pnpm db:up` once. `vitest.config.ts` loads `.env`, which holds `DATABASE_URL=postgres://csv:csv@localhost:5544/csv_import`. Copy `.env.example` to `.env` if it's missing. **Never commit `.env`.**
- **How the pipeline works** (`src/import.ts`): read stream → csv-parse → `validateRows` → `batch` → sink loop. The stages pull data, so when a batch reaches the sink, `position` points just past that batch's last row. The checkpoint relies on that.
- **Current guarantee:** at-least-once plus an idempotent sink. A crash between the batch `COMMIT` and the checkpoint file write replays one batch, and `ON CONFLICT DO NOTHING` absorbs it. This plan removes the replay for Postgres.
- **Baseline:** `pnpm test` → 28 passing (with the database up). `pnpm lint`, `pnpm typecheck` are clean.
- Relevant skills: @superpowers:test-driven-development, @superpowers:verification-before-completion.

---

### Task 0: Branch and baseline

**Step 1: Create a branch**

```bash
cd /Users/baolequoc/workspaces/csv-stream-processor
git checkout master && git pull
git checkout -b feat/production-hardening
```

**Step 2: Start Postgres and confirm the baseline is green**

Run: `pnpm db:up && pnpm test && pnpm lint && pnpm typecheck`
Expected: `Tests  28 passed (28)`. Lint and typecheck print no errors.

If this fails, stop and fix the environment first. Every later "expected" assumes a green start.

---

### Task 1: Fail fast on a wrong header

**Why:** Today a file whose header says `amt` instead of `amount` makes every row fail zod validation. That means 1,000 rejects and then `TooManyInvalidRowsError`, or worse, 2M rejects if `--max-invalid` is high. Checking the header row once, before any data row, gives a single clear error. Unknown columns are rejected too, because zod silently strips unknown keys, so a renamed or extra column would otherwise vanish without a trace.

**Files:**

- Create: `src/pipeline/header.ts`
- Create: `test/header.test.ts`
- Modify: `src/import.ts` (the `parse({ columns: ... })` option, ~line 117)
- Modify: `test/import.test.ts` (add one test at the end of the `describe`)

**Step 1: Write the failing unit test**

Create `test/header.test.ts`:

```ts
import { describe, expect, it } from 'vitest';
import { checkHeader, HeaderMismatchError } from '../src/pipeline/header.ts';

const GOOD = ['id', 'user_email', 'amount', 'currency', 'status', 'created_at', 'description'];

describe('checkHeader', () => {
  it('accepts the expected columns in any order', () => {
    const shuffled = [...GOOD].reverse();
    expect(checkHeader(shuffled)).toEqual(shuffled);
  });

  it('lists missing and unexpected columns', () => {
    const header = GOOD.map((c) => (c === 'amount' ? 'amt' : c));
    expect(() => checkHeader(header)).toThrow(
      'CSV header does not match the expected columns (missing: amount; unexpected: amt)',
    );
  });

  it('treats a repeated column as unexpected', () => {
    try {
      checkHeader([...GOOD, 'id']);
      expect.unreachable();
    } catch (error) {
      expect(error).toBeInstanceOf(HeaderMismatchError);
      expect(error).toMatchObject({ missing: [], unexpected: ['id (duplicate)'] });
    }
  });
});
```

**Step 2: Run it and watch it fail**

Run: `pnpm test test/header.test.ts`
Expected: FAIL. `Failed to load url ../src/pipeline/header.ts` (or "Cannot find module").

**Step 3: Implement `src/pipeline/header.ts`**

```ts
import { CSV_COLUMNS } from '../schema.ts';

export class HeaderMismatchError extends Error {
  override name = 'HeaderMismatchError';
  readonly missing: string[];
  readonly unexpected: string[];

  constructor(missing: string[], unexpected: string[]) {
    const problems = [
      missing.length > 0 ? `missing: ${missing.join(', ')}` : '',
      unexpected.length > 0 ? `unexpected: ${unexpected.join(', ')}` : '',
    ].filter(Boolean);
    super(`CSV header does not match the expected columns (${problems.join('; ')})`);
    this.missing = missing;
    this.unexpected = unexpected;
  }
}

/**
 * Used as csv-parse's `columns` callback, so it runs once on the header row before any data
 * row. Without it a misnamed column fails every single row; an extra one is silently dropped
 * by zod.
 */
export function checkHeader(header: string[]): string[] {
  const expected = new Set<string>(CSV_COLUMNS);
  const missing = CSV_COLUMNS.filter((column) => !header.includes(column));
  const unexpected = header.flatMap((column, i) => {
    if (header.indexOf(column) !== i) return [`${column} (duplicate)`];
    return expected.has(column) ? [] : [column];
  });
  if (missing.length > 0 || unexpected.length > 0) {
    throw new HeaderMismatchError(missing, unexpected);
  }
  return header;
}
```

**Step 4: Run the unit test**

Run: `pnpm test test/header.test.ts`
Expected: PASS (3 tests).

**Step 5: Write the failing integration test**

Append inside the `describe('importCsv', ...)` block of `test/import.test.ts`, and add `import { HeaderMismatchError } from '../src/pipeline/header.ts';` to the imports:

```ts
it('rejects a file with a wrong header before reading any row', async () => {
  const filePath = await writeCsv([
    'id,user_email,amt,currency,status,created_at,description',
    row(ids[0]!),
  ]);
  const sink = new MemorySink();

  await expect(
    importCsv({ filePath, sink, rejectsPath: join(dir, 'r.ndjson'), maxInvalidRows: 10 }),
  ).rejects.toBeInstanceOf(HeaderMismatchError);
  expect(sink.batches).toEqual([]);
});
```

**Step 6: Run it and watch it fail**

Run: `pnpm test test/import.test.ts`
Expected: FAIL. The promise _resolves_ (the row is rejected as invalid, not thrown), so you'll see something like `promise resolved "{ processed: 1, invalid: 1, … }" instead of rejecting`.

**Step 7: Wire it into the parser**

In `src/import.ts`, add `import { checkHeader } from './pipeline/header.ts';` and change the `columns` option of `parse({...})`:

```ts
        // A resumed read starts after the header, so reuse the header saved in the checkpoint
        // (it was checked on the first run). csv-parse rethrows checkHeader's error unchanged.
        columns: resumeFrom ? resumeFrom.columns : checkHeader,
```

**Step 8: Run the full suite**

Run: `pnpm test`
Expected: PASS, 32 tests (28 + 3 + 1). `test/resume.test.ts` must still pass, because its header is the standard `HEADER` from `test/helpers.ts`.

**Step 9: Commit**

```bash
git add src/pipeline/header.ts src/import.ts test/header.test.ts test/import.test.ts
git commit -m "Fail fast when the CSV header does not match the expected columns"
```

---

### Task 2: Let a sink store the checkpoint in its own transaction

**Why:** This is the contract change only. It's tested with an in-memory sink, so it needs no database. A sink that exposes `checkpoints` receives the checkpoint in `writeBatch(rows, checkpoint)` and must commit both atomically. The importer then no longer writes the checkpoint file for that sink. A crash right after a commit therefore can't replay that batch.

**Files:**

- Modify: `src/sinks/types.ts`
- Modify: `src/import.ts` (identity/lock block ~line 58–64, batch loop ~line 129–144, finish ~line 150, `findResumableCheckpoint` ~line 165)
- Modify: `test/resume.test.ts`

**Step 1: Write the failing test**

In `test/resume.test.ts`, change the imports to:

```ts
import type { Checkpoint } from '../src/checkpoint.ts';
import type { CheckpointStore, RowSink } from '../src/sinks/types.ts';
```

Add this class below `CountingSink`:

```ts
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
    if (checkpoint) this.saved.set(checkpoint.filePath, checkpoint);
    if (++this.#batches === this.#crashAfterBatch) throw new Error('crash after commit');
  }
  close() {}
}
```

Add this test inside `describe('resume from checkpoint', ...)`:

```ts
it('never replays a batch when the sink commits the checkpoint with the rows', async () => {
  const writes = new Map<string, number>();
  const saved = new Map<string, Checkpoint>();

  // Valid rows in batches of 3: [v0-v2] [v3-v5] [v6-v8] [v9]. Batch 3 commits, then the process dies.
  await expect(
    importCsv({ ...base, sink: new TransactionalSink(writes, saved, 3) }),
  ).rejects.toThrow('crash after commit');
  expect(writes.size).toBe(9);

  const result = await importCsv({ ...base, sink: new TransactionalSink(writes, saved) });

  expect(result).toMatchObject({ resumed: true, processed: 12, invalid: 2, written: 10 });
  expect([...writes.values()].every((n) => n === 1)).toBe(true);
  expect(writes.size).toBe(10);
  expect(saved.size).toBe(0);
  // The sink owns the checkpoint, so no file is written.
  expect(await exists(base.checkpoint!.path)).toBe(false);
});
```

**Step 2: Run it and watch it fail**

Run: `pnpm test test/resume.test.ts`
Expected: FAIL in the new test. `every((n) => n === 1)` is `false`: the file checkpoint still points after batch 2, so v6–v8 are written twice. (If `pnpm typecheck` is run, it also fails on the missing `CheckpointStore` export. That's expected.)

**Step 3: Extend the sink contract in `src/sinks/types.ts`**

Add the import at the top and the new types. Replace the `RowSink` interface:

```ts
import type { Checkpoint } from '../checkpoint.ts';
import type { Transaction } from '../schema.ts';

// ...SinkOpenContext unchanged...

/** Checkpoints kept inside the destination itself, keyed by the absolute path of the file. */
export interface CheckpointStore {
  load(key: string): Promise<Checkpoint | undefined>;
  clear(key: string): Promise<void>;
}

/** Another process holds the import lock for the same file and destination. */
export class ImportInProgressError extends Error {
  override name = 'ImportInProgressError';

  constructor(key: string) {
    super(`another import of ${key} is already running against this destination`);
  }
}

/**
 * Destination for validated rows. The pipeline awaits `writeBatch` before pulling the next
 * batch, so a slow sink automatically slows down file reading instead of filling up memory.
 *
 * Lifecycle: lock → open → writeBatch* → finish (only if the whole file was imported) → close.
 */
export interface RowSink {
  /** Guarantees a single running import per key on this destination; held until close(). */
  lock?(key: string): Promise<void>;
  open?(context: SinkOpenContext): void | Promise<void>;
  /**
   * `checkpoint` is only passed to sinks that have `checkpoints`: they must commit it in the
   * same transaction as `rows`, so progress can never lag behind (or run ahead of) the data.
   */
  writeBatch(rows: Transaction[], checkpoint?: Checkpoint): void | Promise<void>;
  finish?(): void | Promise<void>;
  close(): void | Promise<void>;
  count?(): number | Promise<number>;
  checkpoints?: CheckpointStore;
}
```

**Step 4: Use it in `src/import.ts`**

4a. Imports: change the sink import to
`import type { CheckpointStore, RowSink } from './sinks/types.ts';`

4b. Update the doc comment on `ImportOptions.checkpoint`:

```ts
/**
 * Save progress after every committed batch and continue from it on the next run.
 * `target` identifies the destination so a checkpoint is never applied to another database.
 * `path` is only used when the sink has no `checkpoints` store of its own.
 */
```

4c. Replace the line `const resumeFrom = await findResumableCheckpoint(options, identity);` with:

```ts
// Locked before the checkpoint is read, so two workers can never resume from the same one.
await sink.lock?.(identity.filePath);
const resumeFrom = await findResumableCheckpoint(options, identity, sink.checkpoints);
```

4d. Replace the body of the last pipeline stage (`async (source) => { for await ... }`) with:

```ts
      async (source) => {
        for await (const rows of source) {
          const checkpoint: Checkpoint | undefined = options.checkpoint && {
            ...identity,
            columns: position.columns ?? [],
            bytes: position.bytes,
            lines: position.lines,
            rejectsBytes,
            counters: { ...counters, written: counters.written + rows.length },
          };
          if (sink.checkpoints) {
            // Committed atomically with the rows: a crash can no longer replay this batch.
            await sink.writeBatch(rows, checkpoint);
          } else {
            await sink.writeBatch(rows);
            // Saved only after the batch is committed. A crash between commit and save replays
            // one batch on resume, which the idempotent sink absorbs (at-least-once + idempotent).
            if (checkpoint) await saveCheckpoint(options.checkpoint!.path, checkpoint);
          }
          counters.written += rows.length;
        }
      },
```

4e. Replace `if (options.checkpoint) await clearCheckpoint(options.checkpoint.path);` with:

```ts
if (options.checkpoint) {
  await (sink.checkpoints
    ? sink.checkpoints.clear(identity.filePath)
    : clearCheckpoint(options.checkpoint.path));
}
```

4f. Change `findResumableCheckpoint` to take the store:

```ts
async function findResumableCheckpoint(
  options: ImportOptions,
  identity: Pick<Checkpoint, 'filePath' | 'fileSize' | 'fileMtimeMs' | 'target'>,
  store: CheckpointStore | undefined,
): Promise<Checkpoint | undefined> {
  if (!options.checkpoint || options.fresh) return undefined;
  const saved = store
    ? await store.load(identity.filePath)
    : await loadCheckpoint(options.checkpoint.path);
  // ...the identity comparison below stays exactly as it is...
```

**Step 5: Run the tests**

Run: `pnpm test && pnpm typecheck && pnpm lint`
Expected: all tests pass (33), no type or lint errors. The three existing resume tests prove the file-based path still works.

**Step 6: Commit**

```bash
git add src/sinks/types.ts src/import.ts test/resume.test.ts
git commit -m "Let sinks commit the checkpoint in the same transaction as the batch"
```

---

### Task 3: Postgres stores the checkpoint transactionally

**Why:** This makes the contract from Task 2 real. One `INSERT … ON CONFLICT DO UPDATE` per batch (about 490 for a 2.4M-row file) costs well under 1 % of the import time.

**Files:**

- Modify: `src/sinks/postgres.ts` (full replacement below)
- Modify: `test/postgres.test.ts`

**Step 1: Write the failing test**

In `test/postgres.test.ts` add the import `import type { Checkpoint } from '../src/checkpoint.ts';` and this helper below `const table = …`:

```ts
const FILE = '/data/a.csv';
const checkpointAt = (written: number): Checkpoint => ({
  filePath: FILE,
  fileSize: 1_000,
  fileMtimeMs: 1_700_000_000_000.5,
  target: 'test',
  columns: ['id', 'amount'],
  bytes: written * 10,
  lines: written + 1,
  rejectsBytes: 0,
  counters: { processed: written, invalid: 0, written },
});
```

Add the test inside the `describe`:

```ts
it('commits the checkpoint in the same transaction as the batch', async () => {
  const ckptTable = `${table}_ckpt`;
  const sink = new PostgresSink(databaseUrl!, { table: ckptTable });
  try {
    expect(await sink.checkpoints.load(FILE)).toBeUndefined();
    await sink.open();
    await sink.writeBatch([tx(1)], checkpointAt(1));
    expect(await sink.checkpoints.load(FILE)).toEqual(checkpointAt(1));

    // 'TOOLONG' overflows char(3), so the COPY fails: neither the row nor the newer checkpoint lands.
    await expect(
      sink.writeBatch([tx(2, { currency: 'TOOLONG' as 'USD' })], checkpointAt(2)),
    ).rejects.toThrow(/too long/);
    expect(await sink.checkpoints.load(FILE)).toEqual(checkpointAt(1));
    expect(await sink.count()).toBe(1);

    await sink.checkpoints.clear(FILE);
    expect(await sink.checkpoints.load(FILE)).toBeUndefined();
  } finally {
    await sink.close();
    const client = new pg.Client({ connectionString: databaseUrl });
    await client.connect();
    await client.query(`DROP TABLE IF EXISTS ${ckptTable}`);
    await client.end();
  }
});
```

**Step 2: Run it and watch it fail**

Run: `pnpm test test/postgres.test.ts`
Expected: FAIL. `TypeError: Cannot read properties of undefined (reading 'load')`.

**Step 3: Replace `src/sinks/postgres.ts`**

Keep the file's top part (imports, `COLUMNS`, `toCsvLine`, `UNIQUE_VIOLATION`, `PostgresSinkStats`, the class doc comment) and the `#copy`/`count` methods unchanged. Add `import type { Checkpoint } from '../checkpoint.ts';` and `type CheckpointStore` to the `./types.ts` import. Then make the class look like this:

```ts
const pgCode = (error: unknown): string | undefined => (error as { code?: string }).code;

export class PostgresSink implements RowSink {
  readonly #client: pg.Client;
  readonly #table: string;
  #connected: Promise<void> | undefined;
  readonly stats: PostgresSinkStats = { directBatches: 0, mergedBatches: 0 };

  /** Checkpoints live next to the data, so `writeBatch` can commit both in one transaction. */
  readonly checkpoints: CheckpointStore = {
    load: async (key) => {
      await this.#connect();
      const { rows } = await this.#client.query<{ checkpoint: Checkpoint }>(
        'SELECT checkpoint FROM import_checkpoints WHERE key = $1',
        [this.#key(key)],
      );
      return rows[0]?.checkpoint;
    },
    clear: async (key) => {
      await this.#connect();
      await this.#client.query('DELETE FROM import_checkpoints WHERE key = $1', [this.#key(key)]);
    },
  };

  constructor(connectionString: string, { table = 'transactions' }: { table?: string } = {}) {
    // The table name is interpolated into SQL, so only allow plain identifiers.
    if (!/^[a-z_][a-z0-9_]*$/.test(table)) throw new Error(`invalid table name: ${table}`);
    this.#table = table;
    this.#client = new pg.Client({ connectionString, application_name: 'csv-stream-processor' });
  }

  async open(): Promise<void> {
    await this.#connect();
    // ...the existing CREATE TABLE IF NOT EXISTS ${this.#table} and CREATE TEMP TABLE staging
    //    statements, unchanged...
  }

  async writeBatch(rows: Transaction[], checkpoint?: Checkpoint): Promise<void> {
    // One chunk per batch rather than one per row: fewer CopyData messages on the wire.
    const csv = rows.map(toCsvLine).join('');
    const path = await this.#writeTransaction(csv, checkpoint);
    // Counted after COMMIT, so a rolled-back attempt is never counted.
    this.stats[path === 'direct' ? 'directBatches' : 'mergedBatches']++;
  }

  async #writeTransaction(csv: string, checkpoint?: Checkpoint): Promise<'direct' | 'merged'> {
    await this.#client.query('BEGIN');
    try {
      const path = await this.#copyBatch(csv);
      if (checkpoint) {
        await this.#client.query(
          `INSERT INTO import_checkpoints (key, checkpoint) VALUES ($1, $2::jsonb)
           ON CONFLICT (key) DO UPDATE SET checkpoint = excluded.checkpoint, updated_at = now()`,
          [this.#key(checkpoint.filePath), JSON.stringify(checkpoint)],
        );
      }
      await this.#client.query('COMMIT');
      return path;
    } catch (error) {
      await this.#client.query('ROLLBACK');
      throw error;
    }
  }

  async #copyBatch(csv: string): Promise<'direct' | 'merged'> {
    await this.#client.query('SAVEPOINT direct_copy');
    try {
      await this.#copy(this.#table, csv);
      return 'direct';
    } catch (error) {
      if (pgCode(error) !== UNIQUE_VIOLATION) throw error;
      await this.#client.query('ROLLBACK TO SAVEPOINT direct_copy');
      await this.#copy('staging', csv);
      await this.#client.query(
        `INSERT INTO ${this.#table} (${COLUMNS}) SELECT ${COLUMNS} FROM staging ON CONFLICT (id) DO NOTHING`,
      );
      return 'merged';
    }
  }

  // ...#copy and count unchanged...

  async close(): Promise<void> {
    // The CLI closes the sink even when the import failed before connecting.
    if (this.#connected) await this.#client.end();
  }

  /** Connects once, from whichever of lock / checkpoints.load / open runs first. */
  #connect(): Promise<void> {
    this.#connected ??= (async () => {
      await this.#client.connect();
      await this.#client.query(`
        CREATE TABLE IF NOT EXISTS import_checkpoints (
          key        text        PRIMARY KEY,
          checkpoint jsonb       NOT NULL,
          updated_at timestamptz NOT NULL DEFAULT now()
        )
      `);
    })();
    return this.#connected;
  }

  /** Includes the table, so imports of the same file into different tables don't collide. */
  #key(filePath: string): string {
    return `${this.#table}:${filePath}`;
  }
}
```

Also update the class doc comment with one sentence:
`Each batch also upserts its checkpoint into import_checkpoints before COMMIT, so a resumed import starts exactly after the last committed batch.`

**Step 4: Run the tests**

Run: `pnpm test && pnpm typecheck && pnpm lint`
Expected: all pass (34). The two existing Postgres tests still assert `stats` `{1,1}` and `{0,1}`, which proves the refactor kept the fast and merge paths.

**Step 5: End-to-end check against real Postgres**

```bash
pnpm generate --out data/hard.csv --size-mb 50
node --env-file-if-exists=.env --disable-warning=ExperimentalWarning src/cli.ts data/hard.csv --sink postgres --fresh
docker compose exec -T postgres psql -U csv -d csv_import -c "SELECT count(*) FROM import_checkpoints"
```

Expected: the import finishes with `written` equal to `rowsInDb` for a fresh table, and `count` is `0` (the checkpoint is cleared at the end). `data/import.checkpoint.json` must **not** exist: `ls data/import.checkpoint.json` gives "No such file".

**Step 6: Commit**

```bash
git add src/sinks/postgres.ts test/postgres.test.ts
git commit -m "Store the Postgres checkpoint in the same transaction as each batch"
```

---

### Task 4: One import per file at a time (advisory lock)

**Why:** If a cron job and a retry (or two pods) start on the same file, both would read the same checkpoint and import the same rows concurrently. With the merge path that's still correct, but it's twice the work and the two workers fight over locks. `pg_try_advisory_lock` is held by the connection, so it's released automatically if the process dies. No stale lock files.

**Files:**

- Modify: `src/sinks/postgres.ts`
- Modify: `test/postgres.test.ts`

**Step 1: Write the failing test**

In `test/postgres.test.ts` change the import to
`import { ImportInProgressError } from '../src/sinks/types.ts';` (new line) and add:

```ts
it('lets only one import of a file run at a time', async () => {
  const first = new PostgresSink(databaseUrl!, { table });
  const second = new PostgresSink(databaseUrl!, { table });
  try {
    await first.lock(FILE);
    await expect(second.lock(FILE)).rejects.toBeInstanceOf(ImportInProgressError);
    await second.lock('/data/other.csv');
  } finally {
    await first.close();
    await second.close();
  }

  // Closing the connection released the lock.
  const third = new PostgresSink(databaseUrl!, { table });
  try {
    await third.lock(FILE);
  } finally {
    await third.close();
  }
});
```

**Step 2: Run it and watch it fail**

Run: `pnpm test test/postgres.test.ts`
Expected: FAIL. `TypeError: first.lock is not a function`.

**Step 3: Implement `lock` in `PostgresSink`**

Add `ImportInProgressError` to the `./types.ts` import, and add this method above `open()`:

```ts
  async lock(key: string): Promise<void> {
    await this.#connect();
    // Session-level lock: released by close(), or by Postgres when the process dies.
    const { rows } = await this.#client.query<{ locked: boolean }>(
      'SELECT pg_try_advisory_lock(hashtextextended($1, 0)) AS locked',
      [this.#key(key)],
    );
    if (!rows[0]!.locked) throw new ImportInProgressError(key);
  }
```

**Step 4: Run the tests**

Run: `pnpm test && pnpm typecheck && pnpm lint`
Expected: all pass (35).

**Step 5: Commit**

```bash
git add src/sinks/postgres.ts test/postgres.test.ts
git commit -m "Take a Postgres advisory lock so a file is imported by one worker at a time"
```

---

### Task 5: Retry batches that Postgres rolled back on a conflict

**Why:** `40001` (serialization failure) and `40P01` (deadlock detected) mean "the transaction was rolled back, try again". They can happen when two different files with overlapping ids load into the same table. Because each batch is one transaction, retrying the whole batch is safe. **Lost connections are deliberately not retried in-process:** the advisory lock and temp table die with the session. The right recovery is to exit non-zero and let a rerun resume from the checkpoint, which Task 3 made exact.

**Files:**

- Create: `src/retry.ts`
- Create: `test/retry.test.ts`
- Modify: `src/sinks/postgres.ts` (`writeBatch`)

**Step 1: Write the failing test**

Create `test/retry.test.ts`:

```ts
import { describe, expect, it } from 'vitest';
import { withRetry } from '../src/retry.ts';

const retryable = Object.assign(new Error('deadlock'), { code: '40P01' });
const fatal = Object.assign(new Error('bad data'), { code: '22001' });
const options = {
  attempts: 3,
  baseDelayMs: 1,
  isRetryable: (e: unknown) => (e as { code?: string }).code === '40P01',
};

/** Fails with the given errors in order, then succeeds. */
function flaky(errors: Error[]) {
  let calls = 0;
  const work = () => {
    calls++;
    const error = errors.shift();
    return error ? Promise.reject(error) : Promise.resolve('ok');
  };
  return { work, calls: () => calls };
}

describe('withRetry', () => {
  it('retries retryable errors until the work succeeds', async () => {
    const f = flaky([retryable, retryable]);
    await expect(withRetry(f.work, options)).resolves.toBe('ok');
    expect(f.calls()).toBe(3);
  });

  it('gives up after the last attempt with the last error', async () => {
    const f = flaky([retryable, retryable, retryable]);
    await expect(withRetry(f.work, options)).rejects.toBe(retryable);
    expect(f.calls()).toBe(3);
  });

  it('does not retry other errors', async () => {
    const f = flaky([fatal]);
    await expect(withRetry(f.work, options)).rejects.toBe(fatal);
    expect(f.calls()).toBe(1);
  });
});
```

**Step 2: Run it and watch it fail**

Run: `pnpm test test/retry.test.ts`
Expected: FAIL. `Failed to load url ../src/retry.ts`.

**Step 3: Implement `src/retry.ts`**

```ts
import { setTimeout } from 'node:timers/promises';

export interface RetryOptions {
  /** Total tries, including the first one. */
  attempts: number;
  baseDelayMs: number;
  isRetryable: (error: unknown) => boolean;
}

export async function withRetry<T>(work: () => Promise<T>, options: RetryOptions): Promise<T> {
  for (let attempt = 1; ; attempt++) {
    try {
      return await work();
    } catch (error) {
      if (attempt >= options.attempts || !options.isRetryable(error)) throw error;
      // Exponential backoff with full jitter, so two conflicting importers don't retry in lockstep.
      await setTimeout(Math.random() * options.baseDelayMs * 2 ** (attempt - 1));
    }
  }
}
```

**Step 4: Run it**

Run: `pnpm test test/retry.test.ts`
Expected: PASS (3 tests).

**Step 5: Use it in `PostgresSink.writeBatch`**

Add `import { withRetry } from '../retry.ts';` and, next to `UNIQUE_VIOLATION`:

```ts
/** serialization_failure, deadlock_detected: Postgres rolled back, the same batch can be retried. */
const RETRYABLE = new Set(['40001', '40P01']);
```

Change the middle line of `writeBatch`:

```ts
const path = await withRetry(() => this.#writeTransaction(csv, checkpoint), {
  attempts: 3,
  baseDelayMs: 100,
  isRetryable: (error) => RETRYABLE.has(pgCode(error) ?? ''),
});
```

There's no Postgres test for this: reliably provoking a deadlock in a test is flaky, and the wiring is one call. The unit tests cover the behaviour.

**Step 6: Run everything and commit**

Run: `pnpm test && pnpm typecheck && pnpm lint`
Expected: all pass (38).

```bash
git add src/retry.ts test/retry.test.ts src/sinks/postgres.ts
git commit -m "Retry a batch when Postgres rolls it back for a serialization failure or deadlock"
```

---

### Task 6: Clean shutdown on SIGTERM and clear exit codes

**Why:** Docker, Kubernetes, ECS and systemd stop a process with **SIGTERM**, not Ctrl+C. Today SIGTERM kills the process immediately. The data is still safe (the in-flight batch rolls back), but the exit looks like a crash. A grace timer guarantees that a stuck query can't hang a container stop forever. Exit code **75** (`EX_TEMPFAIL`, "try again later") tells an orchestrator that a locked file is a temporary failure, not a bug.

**Files:**

- Modify: `src/cli.ts` (the SIGINT block ~line 72–78 and the `catch` ~line 95–104)

**Step 1: Replace the SIGINT handler**

Add `import { ImportInProgressError } from './sinks/types.ts';`, then replace the block from the `// Ctrl+C stops reading cleanly…` comment through `});` with:

```ts
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
```

**Step 2: Update the `catch` block**

```ts
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
}
```

**Step 3: Typecheck and lint**

Run: `pnpm typecheck && pnpm lint`
Expected: clean.

**Step 4: Verify SIGTERM by hand**

Run node directly, not `pnpm import`, because `kill` would hit pnpm instead of the importer:

```bash
CLI="node --env-file-if-exists=.env --disable-warning=ExperimentalWarning src/cli.ts"
pnpm generate --out data/hard.csv --size-mb 300
$CLI data/hard.csv --sink postgres --fresh & PID=$!
sleep 5; kill -TERM $PID; wait $PID; echo "exit=$?"
$CLI data/hard.csv --sink postgres
```

Expected: `SIGTERM received, stopping import...`, then `Import aborted…` and `exit=143`. The second run prints `resumed: true` and `written` is the full valid row count (≈2,453,500 for 300 MB with default invalid ratio).

**Step 5: Verify the lock by hand**

```bash
$CLI data/hard.csv --sink postgres --fresh & PID=$!
sleep 2; $CLI data/hard.csv --sink postgres; echo "exit=$?"
kill -TERM $PID; wait $PID
```

Expected: the second command prints `another import of /…/data/hard.csv is already running against this destination` and `exit=75`.

**Step 6: Commit**

```bash
git add src/cli.ts
git commit -m "Handle SIGTERM like Ctrl+C and exit 75 when the file is already being imported"
```

---

### Task 7: CI with a real Postgres

**Why:** Without CI the Postgres tests only run on a laptop that remembered `pnpm db:up`. The guard makes them **fail** instead of silently skipping when CI is missing the database.

**Files:**

- Create: `.github/workflows/ci.yml`
- Modify: `test/postgres.test.ts` (top of file)

**Step 1: Make CI refuse to skip the Postgres tests**

In `test/postgres.test.ts`, below `const databaseUrl = process.env.DATABASE_URL;`:

```ts
// Locally these tests skip without a database; in CI a skip would hide a broken setup.
if (process.env.CI && !databaseUrl) throw new Error('DATABASE_URL must be set in CI');
```

**Step 2: Create `.github/workflows/ci.yml`**

```yaml
name: CI

on:
  push:
    branches: [master, main]
  pull_request:

jobs:
  test:
    runs-on: ubuntu-latest
    services:
      postgres:
        image: postgres:17-alpine
        env:
          POSTGRES_USER: csv
          POSTGRES_PASSWORD: csv
          POSTGRES_DB: csv_import
        ports:
          - 5432:5432
        options: >-
          --health-cmd "pg_isready -U csv -d csv_import"
          --health-interval 2s
          --health-retries 15
    env:
      DATABASE_URL: postgres://csv:csv@localhost:5432/csv_import
    steps:
      - uses: actions/checkout@v4
      # Reads the pnpm version from "packageManager" in package.json.
      - uses: pnpm/action-setup@v4
      - uses: actions/setup-node@v4
        with:
          node-version-file: .nvmrc
          cache: pnpm
      - run: pnpm install --frozen-lockfile
      - run: pnpm lint
      - run: pnpm typecheck
      - run: pnpm exec prettier --check .
      - run: pnpm test
```

**Step 3: Run the same checks locally**

Run: `pnpm exec prettier --check . && CI=1 pnpm test`
Expected: Prettier reports "All matched files use Prettier code style!" (if not, run `pnpm format` and include the result in the commit). The tests all pass, and the Postgres ones run rather than skip.

**Step 4: Commit and push the branch**

```bash
git add .github/workflows/ci.yml test/postgres.test.ts
git commit -m "Run lint, typecheck and the full test suite against Postgres in CI"
git push -u origin feat/production-hardening
```

Then check the run: `gh run list --branch feat/production-hardening --limit 1` and `gh run watch`.
Expected: the job is green. If `gh` isn't logged in as `baobeta`, open the repo's Actions tab instead.

---

### Task 8: Benchmark regression check and docs

**Files:**

- Modify: `README.md`
- Modify: `docs/design-and-benchmarks.md`

**Step 1: Confirm there's no speed regression**

```bash
/usr/bin/time -p node --env-file-if-exists=.env --disable-warning=ExperimentalWarning \
  src/cli.ts data/hard.csv --sink postgres --fresh
```

Run it twice, on a fresh table: `docker compose exec -T postgres psql -U csv -d csv_import -c "TRUNCATE transactions"` before each run.
Expected: `real` falls in the same range as the earlier measurements of this sink on a 300 MB file (≈37–60 s, depending on the session). Anything far outside that means a regression, so stop and investigate before documenting it.

**Step 2: Update the README**

Find the resume and Postgres sections with `grep -n -i "resume\|checkpoint\|exit code\|SIGINT" README.md`, and update them to say:

- With `--sink postgres` the checkpoint lives in the `import_checkpoints` table and is committed with each batch. Resuming never replays a batch. `--checkpoint` is used only for SQLite.
- Only one import per file and table runs at a time. A second one exits with code **75**.
- SIGTERM behaves like Ctrl+C (exit **143**; Ctrl+C stays **130**), with a 30-second grace period.
- A header that doesn't match the expected columns aborts before any row is read.

**Step 3: Add a decision record to `docs/design-and-benchmarks.md`**

Append it after the last existing decision record, numbered one higher. Use the same heading style as the existing records (check with `grep -n "^### " docs/design-and-benchmarks.md`):

```markdown
### Checkpoint inside the database transaction

**Decision:** for Postgres, the checkpoint is an `import_checkpoints` row upserted in the same
transaction as the batch, guarded by an advisory lock per file and table.

**Why:** a checkpoint file is a second write after the database commit. A crash between the two
replays one batch; `ON CONFLICT DO NOTHING` hid that, but it is wasted work, and it only stays
correct as long as every row is idempotent. One transaction makes rows and progress commit
together or not at all.

**Trade-off:** one extra small statement per batch (~490 per 2.4M rows, under 1 % of the import),
and the importer now creates a table of its own. SQLite still uses the file, because its sink has
no store yet.
```

**Step 4: Format, test, commit, push**

```bash
pnpm format && pnpm test && pnpm lint && pnpm typecheck
git add README.md docs/design-and-benchmarks.md
git commit -m "Document transactional checkpoints, the import lock and the new exit codes"
git push
```

Expected: everything green, CI green on the pushed commit. Then open a PR to `master` (`gh pr create --fill`) only if the user asks for one.

---

## Deliberately out of scope

| Item                                                    | Why not now                                                                                                                                                                                      |
| ------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| S3 or object storage source                             | Skipped at the user's request. The byte-offset checkpoint already fits an S3 `Range` read later.                                                                                                 |
| **What a repeated id means** (skip vs update vs reject) | A business rule, not a technical one. The behaviour today is skip (`ON CONFLICT DO NOTHING`). Decide it with whoever owns the data before changing `#copyBatch`.                                 |
| Migration tool                                          | The sink still runs `CREATE TABLE IF NOT EXISTS`. Two workers starting on an empty database at the same instant can race on it. Move the DDL to node-pg-migrate when the schema starts changing. |
| Transactional checkpoint for SQLite                     | Easy to add the same way, but SQLite is the learning or local path. YAGNI until someone runs it in production.                                                                                   |
| Structured logs and metrics                             | Next plan: pino with `job_id` on every line, and counters for rows/s, rejects and merged batches.                                                                                                |
