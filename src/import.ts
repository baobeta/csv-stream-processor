import {
  closeSync,
  createReadStream,
  fstatSync,
  ftruncateSync,
  openSync,
  writeSync,
} from 'node:fs';
import { stat } from 'node:fs/promises';
import { resolve } from 'node:path';
import { pipeline } from 'node:stream/promises';
import { parse, type Parser } from 'csv-parse';
import { clearCheckpoint, loadCheckpoint, saveCheckpoint, type Checkpoint } from './checkpoint.ts';
import { batch } from './pipeline/batch.ts';
import { checkHeader } from './pipeline/header.ts';
import type { ErrorPolicy, ImportCounters } from './pipeline/error-policy.ts';
import { validateRows, type StreamPosition } from './pipeline/validate.ts';
import type { CheckpointStore, RowSink } from './sinks/types.ts';

export interface ImportOptions {
  filePath: string;
  sink: RowSink;
  rejectsPath: string;
  batchSize?: number;
  maxInvalidRows?: number;
  /**
   * Save progress after every committed batch and continue from it on the next run.
   * `target` identifies the destination so a checkpoint is never applied to another database.
   * `path` is only used when the sink has no `checkpoints` store of its own.
   */
  checkpoint?: { path: string; target: string };
  /** Ignore an existing checkpoint and start from the first row. */
  fresh?: boolean;
  /** Abort the import (e.g. on SIGINT). The current batch transaction is rolled back or committed atomically. */
  signal?: AbortSignal;
  onProgress?: (progress: ImportProgress) => void;
  progressIntervalMs?: number;
}

export interface ImportProgress extends ImportCounters {
  bytesRead: number;
  totalBytes: number;
  elapsedMs: number;
  /** Where this run started (non-zero when resuming), so rates only count this run's work. */
  startedAt: { bytes: number; processed: number };
}

export interface ImportResult extends ImportCounters {
  durationMs: number;
  resumed: boolean;
}

/** 1 MiB reads instead of the 64 KiB default: fewer syscalls, still constant memory. */
const READ_CHUNK_BYTES = 1024 * 1024;
/** A record larger than this almost always means an unbalanced quote swallowing the file. */
const MAX_RECORD_BYTES = 64 * 1024;

export async function importCsv(options: ImportOptions): Promise<ImportResult> {
  const { filePath, sink, batchSize = 5_000, maxInvalidRows = 1_000 } = options;

  const file = await stat(filePath);
  const identity = {
    filePath: resolve(filePath),
    fileSize: file.size,
    fileMtimeMs: file.mtimeMs,
    target: options.checkpoint?.target ?? '',
  };
  // Locked before the checkpoint is read, so two workers can never resume from the same one.
  await sink.lock?.(identity.filePath);
  const resumeFrom = await findResumableCheckpoint(options, identity, sink.checkpoints);

  const counters: ImportCounters = resumeFrom
    ? { ...resumeFrom.counters }
    : { processed: 0, invalid: 0, written: 0 };
  const position: StreamPosition = resumeFrom
    ? { bytes: resumeFrom.bytes, lines: resumeFrom.lines, columns: resumeFrom.columns }
    : { bytes: 0, lines: 0 };
  const startedAt = { bytes: position.bytes, processed: counters.processed };

  const input = createReadStream(filePath, {
    highWaterMark: READ_CHUNK_BYTES,
    start: position.bytes,
  });
  // Rejects are rare, so they are appended synchronously: the size on disk then always matches
  // the size recorded in the checkpoint. On resume, rejects written after the last checkpoint
  // are cut off, because those rows are about to be read and rejected again.
  const rejectsFd = openSync(options.rejectsPath, resumeFrom ? 'a' : 'w');
  let rejectsBytes = 0;
  if (resumeFrom) {
    rejectsBytes = Math.min(resumeFrom.rejectsBytes, fstatSync(rejectsFd).size);
    ftruncateSync(rejectsFd, rejectsBytes);
  }
  const policy: ErrorPolicy = {
    maxInvalidRows,
    reject: (row) => {
      rejectsBytes += writeSync(rejectsFd, `${JSON.stringify(row)}\n`);
    },
  };

  const checkpointPath = options.checkpoint?.path;
  const startedAtMs = performance.now();
  const reportProgress = () =>
    options.onProgress?.({
      ...counters,
      bytesRead: startedAt.bytes + input.bytesRead,
      totalBytes: file.size,
      elapsedMs: performance.now() - startedAtMs,
      startedAt,
    });
  const timer = options.onProgress
    ? setInterval(reportProgress, options.progressIntervalMs ?? 1_000)
    : undefined;

  try {
    await sink.open?.({ resuming: resumeFrom !== undefined });

    // pipeline() wires backpressure between every stage and destroys all of them if any
    // stage throws or the signal aborts - no leaked file descriptors, no dangling listeners.
    await pipeline(
      input,
      parse({
        // A resumed read starts after the header, so reuse the header saved in the checkpoint
        // (it was checked on the first run). csv-parse rethrows checkHeader's error unchanged.
        columns: resumeFrom ? resumeFrom.columns : checkHeader,
        bom: true,
        skip_empty_lines: true,
        trim: true,
        info: true,
        max_record_size: MAX_RECORD_BYTES,
      }),
      // destroyOnReturn: false - when validateRows throws, the default iterator would destroy the
      // parser with a generic AbortError that reaches pipeline() first and masks the real error
      // (e.g. TooManyInvalidRowsError). pipeline() still destroys the parser with the real one.
      (source: Parser) =>
        validateRows(source.iterator({ destroyOnReturn: false }), counters, policy, position),
      (source) => batch(source, batchSize),
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
            if (checkpoint && checkpointPath) await saveCheckpoint(checkpointPath, checkpoint);
          }
          counters.written += rows.length;
        }
      },
      { signal: options.signal },
    );

    await sink.finish?.();
    if (options.checkpoint) {
      await (sink.checkpoints
        ? sink.checkpoints.clear(identity.filePath)
        : clearCheckpoint(options.checkpoint.path));
    }
    reportProgress();
  } finally {
    clearInterval(timer);
    closeSync(rejectsFd);
  }

  return {
    ...counters,
    durationMs: performance.now() - startedAtMs,
    resumed: resumeFrom !== undefined,
  };
}

async function findResumableCheckpoint(
  options: ImportOptions,
  identity: Pick<Checkpoint, 'filePath' | 'fileSize' | 'fileMtimeMs' | 'target'>,
  store: CheckpointStore | undefined,
): Promise<Checkpoint | undefined> {
  if (!options.checkpoint || options.fresh) return undefined;
  const saved = store
    ? await store.load(identity.filePath)
    : await loadCheckpoint(options.checkpoint.path);
  // A checkpoint for a different or modified file (or another database) would point at
  // meaningless byte offsets, so it is ignored and overwritten by this run.
  const matches =
    saved !== undefined &&
    saved.filePath === identity.filePath &&
    saved.fileSize === identity.fileSize &&
    saved.fileMtimeMs === identity.fileMtimeMs &&
    saved.target === identity.target;
  return matches ? saved : undefined;
}
