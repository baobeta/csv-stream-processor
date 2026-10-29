import { CSV_COLUMNS } from '../schema.ts';

export class HeaderMismatchError extends Error {
  override name = 'HeaderMismatchError';
  readonly missing: string[];
  readonly unexpected: string[];

  constructor(missing: string[], unexpected: string[]) {
    const problems = [
      missing.length > 0 ? `missing: ${missing.join(', ')}` : '',
      unexpected.length > 0 ? `unexpected: ${unexpected.join(', ')}` : '',
    ].filter(Boolean);
    super(`CSV header does not match the expected columns (${problems.join('; ')})`);
    this.missing = missing;
    this.unexpected = unexpected;
  }
}

/**
 * Used as csv-parse's `columns` callback, so it runs once on the header row before any data
 * row. Without it a misnamed column fails every single row; an extra one is silently dropped
 * by zod.
 */
export function checkHeader(header: string[]): string[] {
  const expected = new Set<string>(CSV_COLUMNS);
  const missing: string[] = CSV_COLUMNS.filter((column) => !header.includes(column));
  const unexpected = header.flatMap((column, i) => {
    if (header.indexOf(column) !== i) return [`${column} (duplicate)`];
    return expected.has(column) ? [] : [column];
  });
  if (missing.length > 0 || unexpected.length > 0) {
    throw new HeaderMismatchError(missing, unexpected);
  }
  return header;
}
