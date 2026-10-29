import { Readable } from 'node:stream';
import { pipeline } from 'node:stream/promises';
import pg from 'pg';
import { from as copyFrom } from 'pg-copy-streams';
import { csvLine } from '../csv.ts';
import type { Transaction } from '../schema.ts';
import { TRANSACTION_COLUMNS, type RowSink } from './types.ts';

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
 */
export class PostgresSink implements RowSink {
  readonly #client: pg.Client;
  readonly #table: string;
  readonly stats: PostgresSinkStats = { directBatches: 0, mergedBatches: 0 };

  constructor(connectionString: string, { table = 'transactions' }: { table?: string } = {}) {
    // The table name is interpolated into SQL, so only allow plain identifiers.
    if (!/^[a-z_][a-z0-9_]*$/.test(table)) throw new Error(`invalid table name: ${table}`);
    this.#table = table;
    this.#client = new pg.Client({ connectionString });
  }

  async open(): Promise<void> {
    await this.#client.connect();
    await this.#client.query(`
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

  async writeBatch(rows: Transaction[]): Promise<void> {
    // One chunk per batch rather than one per row: fewer CopyData messages on the wire.
    const csv = rows.map(toCsvLine).join('');
    await this.#client.query('BEGIN');
    try {
      await this.#client.query('SAVEPOINT direct_copy');
      try {
        await this.#copy(this.#table, csv);
        this.stats.directBatches++;
      } catch (error) {
        if ((error as { code?: string }).code !== UNIQUE_VIOLATION) throw error;
        await this.#client.query('ROLLBACK TO SAVEPOINT direct_copy');
        await this.#copy('staging', csv);
        await this.#client.query(
          `INSERT INTO ${this.#table} (${COLUMNS}) SELECT ${COLUMNS} FROM staging ON CONFLICT (id) DO NOTHING`,
        );
        this.stats.mergedBatches++;
      }
      await this.#client.query('COMMIT');
    } catch (error) {
      await this.#client.query('ROLLBACK');
      throw error;
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
    await this.#client.end();
  }
}
