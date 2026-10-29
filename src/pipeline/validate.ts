import type { Info } from 'csv-parse';
import type { z } from 'zod';
import { transactionSchema, type Transaction } from '../schema.ts';
import { handleInvalidRow, type ErrorPolicy, type ImportCounters } from './error-policy.ts';

/** Shape emitted by csv-parse when `info: true` and `columns: true`. */
export interface ParsedRecord {
  record: Record<string, string>;
  info: Info;
}

/**
 * Where the pipeline is in the file. `validateRows` updates it after every record; because the
 * downstream stages are pull-based generators, when a batch reaches the sink this points just
 * past the batch's last row - exactly what a checkpoint needs.
 */
export interface StreamPosition {
  bytes: number;
  lines: number;
  columns?: string[];
}

function formatIssues(error: z.ZodError): string {
  return error.issues.map((issue) => `${issue.path.join('.')}: ${issue.message}`).join('; ');
}

export async function* validateRows(
  source: AsyncIterable<ParsedRecord>,
  counters: ImportCounters,
  policy: ErrorPolicy,
  position: StreamPosition,
): AsyncGenerator<Transaction> {
  // csv-parse counts from wherever the read stream started; add the resume offset back.
  const base = { bytes: position.bytes, lines: position.lines };

  for await (const { record, info } of source) {
    counters.processed++;
    position.bytes = base.bytes + info.bytes_records;
    position.lines = base.lines + info.lines;
    position.columns ??= Object.keys(record);

    const result = transactionSchema.safeParse(record);
    if (result.success) {
      yield result.data;
      continue;
    }

    handleInvalidRow(
      { line: position.lines, reason: formatIssues(result.error), raw: record },
      counters,
      policy,
    );
  }
}
