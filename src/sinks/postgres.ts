import { Readable } from 'node:stream';
import { pipeline } from 'node:stream/promises';
import pg from 'pg';
import { from as copyFrom } from 'pg-copy-streams';
import type { Checkpoint } from '../checkpoint.ts';
import { csvLine } from '../csv.ts';
import type { Transaction } from '../schema.ts';
import {
  ImportInProgressError,
  TRANSACTION_COLUMNS,
  type CheckpointStore,
  type RowSink,
} from './types.ts';

const COLUMNS = TRANSACTION_COLUMNS.join(', ');

const toCsvLine = (r: Transaction): string =>
  csvLine([
    r.id,
    r.user_email,
    String(r.amount),
    r.currency,
    r.status,
    r.created_at,
    r.description,
  ]);

/** Postgres error code for a unique constraint violation. */
const UNIQUE_VIOLATION = '23505';

const pgCode = (error: unknown): string | undefined => (error as { code?: string }).code;

export interface PostgresSinkStats {
  /** Batches loaded with a plain COPY into the target table (the fast path). */
  directBatches: number;
  /** Batches that contained already-stored ids and went through the staging merge. */
  mergedBatches: number;
}

/**
 * Loads batches with `COPY ... FROM STDIN`, the fastest way to get bulk data into Postgres.
 *
 * COPY cannot skip duplicates, so each batch is first COPYed straight into the target table
 * inside a savepoint. Only if that hits a duplicate id (a resumed or repeated import) is the
 * batch rolled back to the savepoint and merged through a temp table with
 * `ON CONFLICT DO NOTHING`. Merging every batch that way was ~2.5x slower, because
 * ON CONFLICT checks each row speculatively even when there are no duplicates.
 *
 * Each batch also upserts its checkpoint into import_checkpoints before COMMIT, so a resumed
 * import starts exactly after the last committed batch.
 */
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
    // Without a listener, a connection dropped by the server between queries crashes the process.
    // The next query rejects with a clear error instead, the import fails, and a rerun resumes.
    this.#client.on('error', () => {});
  }

  async lock(key: string): Promise<void> {
    await this.#connect();
    // Session-level lock: released by close(), or by Postgres when the process dies.
    const { rows } = await this.#client.query<{ locked: boolean }>(
      'SELECT pg_try_advisory_lock(hashtextextended($1, 0)) AS locked',
      [this.#key(key)],
    );
    if (!rows[0]!.locked) throw new ImportInProgressError(key);
  }

  async open(): Promise<void> {
    await this.#connect();
    await this.#createIfMissing(`
      CREATE TABLE IF NOT EXISTS ${this.#table} (
        id           uuid        PRIMARY KEY,
        user_email   text        NOT NULL,
        amount_minor bigint      NOT NULL,
        currency     char(3)     NOT NULL,
        status       text        NOT NULL,
        created_at   timestamptz NOT NULL,
        description  text        NOT NULL
      )
    `);
    // Session-local, no indexes, emptied on every COMMIT: each batch starts clean.
    await this.#client.query(
      `CREATE TEMP TABLE staging (LIKE ${this.#table}) ON COMMIT DELETE ROWS`,
    );
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
      await this.#rollback(error);
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

  async #copy(table: string, csv: string): Promise<void> {
    await pipeline(
      Readable.from([csv]),
      this.#client.query(copyFrom(`COPY ${table} (${COLUMNS}) FROM STDIN WITH (FORMAT csv)`)),
    );
  }

  async count(): Promise<number> {
    const { rows } = await this.#client.query<{ n: number }>(
      `SELECT count(*)::int AS n FROM ${this.#table}`,
    );
    return rows[0]!.n;
  }

  async close(): Promise<void> {
    // Only end a client we tried to connect; the CLI closes the sink even when nothing connected.
    if (this.#connected) await this.#client.end();
  }

  /** Connects once, from whichever of lock / checkpoints.load / checkpoints.clear / open runs first. */
  #connect(): Promise<void> {
    this.#connected ??= (async () => {
      await this.#client.connect();
      await this.#createIfMissing(`
        CREATE TABLE IF NOT EXISTS import_checkpoints (
          key        text        PRIMARY KEY,
          checkpoint jsonb       NOT NULL,
          updated_at timestamptz NOT NULL DEFAULT now()
        )
      `);
    })();
    return this.#connected;
  }

  /**
   * IF NOT EXISTS is not safe under concurrency (two sessions both create, the loser gets 23505),
   * so DDL runs behind one global transaction-level advisory lock.
   */
  async #createIfMissing(ddl: string): Promise<void> {
    await this.#client.query('BEGIN');
    try {
      await this.#client.query(
        "SELECT pg_advisory_xact_lock(hashtextextended('csv-stream-processor:ddl', 0))",
      );
      await this.#client.query(ddl);
      await this.#client.query('COMMIT');
    } catch (error) {
      await this.#rollback(error);
      throw error;
    }
  }

  /** Rolls back without masking `error`: a failed ROLLBACK is attached as `rollbackError`. */
  async #rollback(error: unknown): Promise<void> {
    await this.#client.query('ROLLBACK').catch((rollbackError: unknown) => {
      (error as { rollbackError?: unknown }).rollbackError = rollbackError;
    });
  }

  /** Includes the table, so imports of the same file into different tables don't collide. */
  #key(filePath: string): string {
    return `${this.#table}:${filePath}`;
  }
}
