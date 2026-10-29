import type { Transaction } from '../schema.ts';

export interface SinkOpenContext {
  /** True when continuing an interrupted import: rows from the previous run are already stored. */
  resuming: boolean;
}

/**
 * Destination for validated rows. The pipeline awaits `writeBatch` before pulling the next
 * batch, so a slow sink automatically slows down file reading instead of filling up memory.
 *
 * Lifecycle: open → writeBatch* → finish (only if the whole file was imported) → close.
 */
export interface RowSink {
  open?(context: SinkOpenContext): void | Promise<void>;
  writeBatch(rows: Transaction[]): void | Promise<void>;
  finish?(): void | Promise<void>;
  close(): void | Promise<void>;
  count?(): number | Promise<number>;
}

export const TRANSACTION_COLUMNS = [
  'id',
  'user_email',
  'amount_minor',
  'currency',
  'status',
  'created_at',
  'description',
] as const;
