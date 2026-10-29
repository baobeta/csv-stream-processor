export interface RejectedRow {
  /** Physical line in the CSV where the record ends (quoted fields may span several lines). */
  line: number;
  reason: string;
  raw: Record<string, string>;
}

export interface ImportCounters {
  /** Records read from the CSV (valid + invalid). */
  processed: number;
  invalid: number;
  /** Valid rows handed to the sink. */
  written: number;
}

export interface ErrorPolicy {
  /** How many invalid rows are tolerated before the whole import is aborted. 0 = fail fast. */
  maxInvalidRows: number;
  /** Persists a rejected row (e.g. to rejects.ndjson) so it can be fixed and re-imported. */
  reject: (row: RejectedRow) => void;
}

export class TooManyInvalidRowsError extends Error {
  override name = 'TooManyInvalidRowsError';
  readonly invalidCount: number;

  constructor(invalidCount: number, maxInvalidRows: number) {
    super(`aborted: ${invalidCount} invalid rows exceeds the limit of ${maxInvalidRows}`);
    this.invalidCount = invalidCount;
  }
}

/**
 * Decides what happens to a row that failed schema validation. Called once per invalid row,
 * after `counters.processed` has already been incremented.
 *
 * Contract (see test/error-policy.test.ts):
 *   - count the row in `counters.invalid`
 *   - hand it to `policy.reject` so it is not silently lost
 *   - throw `TooManyInvalidRowsError` once the number of invalid rows exceeds
 *     `policy.maxInvalidRows`. Throwing here tears down the whole stream pipeline.
 */
export function handleInvalidRow(
  row: RejectedRow,
  counters: ImportCounters,
  policy: ErrorPolicy,
): void {
  counters.invalid++;
  // Persist before throwing, so the row that crosses the limit still ends up in the rejects file.
  policy.reject(row);
  if (counters.invalid > policy.maxInvalidRows) {
    throw new TooManyInvalidRowsError(counters.invalid, policy.maxInvalidRows);
  }
}
