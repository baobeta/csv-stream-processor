import { readFile, rename, rm, writeFile } from 'node:fs/promises';
import type { ImportCounters } from './pipeline/error-policy.ts';

export interface Checkpoint {
  /** Identifies file + destination; a checkpoint is only reused when every field matches. */
  filePath: string;
  fileSize: number;
  fileMtimeMs: number;
  target: string;
  /** Header of the original file, needed because a resumed read starts after it. */
  columns: string[];
  /** Byte offset just after the last record whose batch was committed. */
  bytes: number;
  /** Physical lines consumed up to that offset (keeps reject line numbers absolute). */
  lines: number;
  /** Size of the rejects file at that point; anything after it is re-produced on resume. */
  rejectsBytes: number;
  counters: ImportCounters;
}

export async function loadCheckpoint(path: string): Promise<Checkpoint | undefined> {
  try {
    return JSON.parse(await readFile(path, 'utf8')) as Checkpoint;
  } catch (error) {
    if ((error as NodeJS.ErrnoException).code === 'ENOENT') return undefined;
    throw error;
  }
}

/** Write-then-rename: rename is atomic, so a crash never leaves a half-written checkpoint. */
export async function saveCheckpoint(path: string, checkpoint: Checkpoint): Promise<void> {
  const tmp = `${path}.tmp`;
  await writeFile(tmp, JSON.stringify(checkpoint));
  await rename(tmp, path);
}

export async function clearCheckpoint(path: string): Promise<void> {
  await rm(path, { force: true });
}
