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

/**
 * Loads batches with `COPY ... FROM STDIN`, the fastest way to get bulk data into Postgres
 * (one round-trip per batch, no per-row parse/plan). COPY cannot skip duplicates, so each
 * batch goes through an index-free temp table and is merged with `ON CONFLICT DO NOTHING`.
 */
export class PostgresSink implements RowSink {
  readonly #client: pg.Client;
  readonly #table: string;

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
    await this.#client.query('BEGIN');
    try {
      await pipeline(
        Readable.from(rows.map(toCsvLine)),
        this.#client.query(copyFrom(`COPY staging (${COLUMNS}) FROM STDIN WITH (FORMAT csv)`)),
      );
      await this.#client.query(
        `INSERT INTO ${this.#table} (${COLUMNS}) SELECT ${COLUMNS} FROM staging ON CONFLICT (id) DO NOTHING`,
      );
      await this.#client.query('COMMIT');
    } catch (error) {
      await this.#client.query('ROLLBACK');
      throw error;
    }
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
