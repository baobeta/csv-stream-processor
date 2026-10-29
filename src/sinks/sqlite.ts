import { DatabaseSync, type StatementSync } from 'node:sqlite';
import type { Transaction } from '../schema.ts';
import { TRANSACTION_COLUMNS, type RowSink, type SinkOpenContext } from './types.ts';

/**
 * - `direct`: insert every batch straight into the indexed table.
 * - `staging`: append batches to an index-free table, then merge everything in one sorted
 *   `INSERT ... SELECT ... ORDER BY id` at the end. Random UUIDv4 keys stop thrashing the
 *   B-tree because the merge visits the index in key order.
 */
export type SqliteStrategy = 'direct' | 'staging';

const COLUMNS = TRANSACTION_COLUMNS.join(', ');

/**
 * Writes batches into SQLite using Node's built-in driver (no native addon to compile).
 */
export class SqliteSink implements RowSink {
  readonly #db: DatabaseSync;
  readonly #insert: StatementSync;
  readonly #strategy: SqliteStrategy;

  constructor(path: string, { strategy = 'direct' }: { strategy?: SqliteStrategy } = {}) {
    this.#strategy = strategy;
    this.#db = new DatabaseSync(path);
    // Random UUIDv4 primary keys land on random B-tree pages, so once the index outgrows the
    // page cache (default ~2 MB) every insert hits disk. A 64 MB cache helps; the staging
    // strategy or time-ordered UUIDv7 keys help much more. See README.
    this.#db.exec(`
      PRAGMA journal_mode = WAL;
      PRAGMA synchronous = NORMAL;
      PRAGMA cache_size = -65536;

      CREATE TABLE IF NOT EXISTS transactions (
        id           TEXT PRIMARY KEY,
        user_email   TEXT    NOT NULL,
        amount_minor INTEGER NOT NULL,
        currency     TEXT    NOT NULL,
        status       TEXT    NOT NULL,
        created_at   TEXT    NOT NULL,
        description  TEXT    NOT NULL
      );

      CREATE TABLE IF NOT EXISTS transactions_staging (
        id           TEXT    NOT NULL,
        user_email   TEXT    NOT NULL,
        amount_minor INTEGER NOT NULL,
        currency     TEXT    NOT NULL,
        status       TEXT    NOT NULL,
        created_at   TEXT    NOT NULL,
        description  TEXT    NOT NULL
      );
    `);
    // ON CONFLICT DO NOTHING makes re-running the same file safe (idempotent import).
    this.#insert = this.#db.prepare(
      strategy === 'direct'
        ? `INSERT INTO transactions (${COLUMNS}) VALUES (?, ?, ?, ?, ?, ?, ?) ON CONFLICT (id) DO NOTHING`
        : `INSERT INTO transactions_staging (${COLUMNS}) VALUES (?, ?, ?, ?, ?, ?, ?)`,
    );
  }

  open({ resuming }: SinkOpenContext): void {
    // Leftovers from an abandoned run must not be merged into a fresh import; on resume they
    // are exactly the rows the checkpoint says were committed.
    if (this.#strategy === 'staging' && !resuming)
      this.#db.exec('DELETE FROM transactions_staging');
  }

  writeBatch(rows: Transaction[]): void {
    // One transaction per batch: thousands of inserts share a single fsync, and a crash
    // never leaves a half-written batch behind.
    this.#transaction(() => {
      for (const r of rows) {
        this.#insert.run(
          r.id,
          r.user_email,
          r.amount,
          r.currency,
          r.status,
          r.created_at,
          r.description,
        );
      }
    });
  }

  finish(): void {
    if (this.#strategy !== 'staging') return;
    this.#transaction(() => {
      // `WHERE true` resolves SQLite's parsing ambiguity between a SELECT clause and ON CONFLICT.
      this.#db.exec(`
        INSERT INTO transactions (${COLUMNS})
        SELECT ${COLUMNS} FROM transactions_staging WHERE true ORDER BY id
        ON CONFLICT (id) DO NOTHING;
        DELETE FROM transactions_staging;
      `);
    });
  }

  count(): number {
    const row = this.#db.prepare('SELECT COUNT(*) AS n FROM transactions').get() as { n: number };
    return row.n;
  }

  close(): void {
    this.#db.close();
  }

  #transaction(work: () => void): void {
    this.#db.exec('BEGIN');
    try {
      work();
      this.#db.exec('COMMIT');
    } catch (error) {
      this.#db.exec('ROLLBACK');
      throw error;
    }
  }
}
