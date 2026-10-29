import type { Checkpoint } from '../checkpoint.ts';
import type { Transaction } from '../schema.ts';

export interface SinkOpenContext {
  /** True when continuing an interrupted import: rows from the previous run are already stored. */
  resuming: boolean;
}

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
  /**
   * Guarantees a single running import per key on this destination (scoped to this sink's
   * destination plus `key`). Released by close(), which the caller of importCsv must call even
   * when lock() throws.
   */
  lock?(key: string): Promise<void>;
  open?(context: SinkOpenContext): void | Promise<void>;
  /**
   * `checkpoint` is only passed to sinks that have `checkpoints`: they must commit it in the
   * same transaction as `rows`, so progress can never lag behind (or run ahead of) the data.
   * It is `undefined` when checkpointing is disabled for this import; when present it must be
   * persisted in the same transaction.
   */
  writeBatch(rows: Transaction[], checkpoint?: Checkpoint): void | Promise<void>;
  finish?(): void | Promise<void>;
  close(): void | Promise<void>;
  count?(): number | Promise<number>;
  /**
   * `lock` and `checkpoints.load` run BEFORE `open()`, so they must work before the destination
   * schema exists (e.g. load returns undefined). `clear` runs after `finish()`.
   */
  checkpoints?: CheckpointStore;
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
