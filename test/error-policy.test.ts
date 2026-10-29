import { describe, expect, it, vi } from 'vitest';
import {
  handleInvalidRow,
  TooManyInvalidRowsError,
  type ImportCounters,
  type RejectedRow,
} from '../src/pipeline/error-policy.ts';

const row: RejectedRow = { line: 2, reason: 'amount: invalid', raw: { amount: '-1' } };

describe('handleInvalidRow', () => {
  it('counts and persists the row while under the limit', () => {
    const counters: ImportCounters = { processed: 1, invalid: 0, written: 0 };
    const reject = vi.fn();

    handleInvalidRow(row, counters, { maxInvalidRows: 5, reject });

    expect(counters.invalid).toBe(1);
    expect(reject).toHaveBeenCalledWith(row);
  });

  it('throws once the limit is exceeded', () => {
    const counters: ImportCounters = { processed: 3, invalid: 2, written: 0 };

    expect(() => handleInvalidRow(row, counters, { maxInvalidRows: 2, reject: vi.fn() })).toThrow(
      TooManyInvalidRowsError,
    );
  });

  it('fails fast on the first invalid row when maxInvalidRows is 0', () => {
    const counters: ImportCounters = { processed: 1, invalid: 0, written: 0 };

    expect(() => handleInvalidRow(row, counters, { maxInvalidRows: 0, reject: vi.fn() })).toThrow(
      TooManyInvalidRowsError,
    );
  });
});
