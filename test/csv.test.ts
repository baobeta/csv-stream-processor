import { describe, expect, it } from 'vitest';
import { csvLine } from '../src/csv.ts';

describe('csvLine', () => {
  it('quotes only when needed and doubles embedded quotes', () => {
    expect(csvLine(['a', 'b, c', 'say "hi"', 'x\ny'])).toBe('a,"b, c","say ""hi""","x\ny"\n');
  });

  it('quotes empty strings so Postgres COPY does not read them as NULL', () => {
    expect(csvLine(['a', ''])).toBe('a,""\n');
  });
});
