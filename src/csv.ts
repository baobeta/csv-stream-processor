/**
 * RFC 4180 quoting: wrap when needed and double any embedded quote.
 * Empty strings are quoted too, because Postgres `COPY ... (FORMAT csv)` reads an unquoted
 * empty field as NULL, not as ''.
 */
export const csvCell = (value: string): string =>
  value === '' || /[",\n\r]/.test(value) ? `"${value.replaceAll('"', '""')}"` : value;

export const csvLine = (cells: readonly string[]): string => `${cells.map(csvCell).join(',')}\n`;
