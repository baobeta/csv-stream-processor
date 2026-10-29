/**
 * Generates a realistic transactions CSV of a given size, including the edge cases a naive
 * `line.split(',')` parser gets wrong (quoted commas, escaped quotes, embedded newlines) and a
 * configurable share of invalid rows.
 *
 *   pnpm generate --size-mb 2048 --invalid-ratio 0.001 [--id-format v4|v7]
 */
import { randomUUID } from 'node:crypto';
import { createWriteStream } from 'node:fs';
import { mkdir } from 'node:fs/promises';
import { dirname } from 'node:path';
import { once } from 'node:events';
import { finished } from 'node:stream/promises';
import { parseArgs } from 'node:util';
import { csvLine } from '../src/csv.ts';
import { CSV_COLUMNS, CURRENCIES, STATUSES } from '../src/schema.ts';
import { uuidv7 } from '../src/uuid.ts';

const { values } = parseArgs({
  options: {
    out: { type: 'string', default: 'data/transactions.csv' },
    'size-mb': { type: 'string', default: '100' },
    'invalid-ratio': { type: 'string', default: '0.001' },
    'id-format': { type: 'string', default: 'v4' },
  },
});

const targetBytes = Number(values['size-mb']) * 1024 * 1024;
const invalidRatio = Number(values['invalid-ratio']);
const ROWS_PER_CHUNK = 1_000;
if (values['id-format'] !== 'v4' && values['id-format'] !== 'v7') {
  throw new Error(`--id-format must be v4 or v7, got ${values['id-format']}`);
}
const newId = values['id-format'] === 'v7' ? () => uuidv7() : () => randomUUID();

const DESCRIPTIONS = [
  'Coffee',
  'Groceries, weekly',
  'Book "Designing Data-Intensive Applications"',
  'Refund\nsee ticket #4521',
  'Subscription',
];
const INVALID_MUTATIONS: Array<(row: string[]) => void> = [
  (row) => (row[1] = 'not-an-email'),
  (row) => (row[2] = '-15.00'),
  (row) => (row[3] = 'XXX'),
  (row) => (row[5] = '2024-13-45'),
];

const pick = <T>(items: readonly T[]): T => items[Math.floor(Math.random() * items.length)]!;

function makeRow(): string {
  const row = [
    newId(),
    `user${Math.floor(Math.random() * 1_000_000)}@example.com`,
    (Math.random() * 10_000).toFixed(2),
    pick(CURRENCIES),
    pick(STATUSES),
    new Date(Date.now() - Math.floor(Math.random() * 3e10)).toISOString(),
    pick(DESCRIPTIONS),
  ];
  if (Math.random() < invalidRatio) pick(INVALID_MUTATIONS)(row);
  return csvLine(row);
}

await mkdir(dirname(values.out), { recursive: true });
const out = createWriteStream(values.out);

let bytesWritten = 0;
let rows = 0;
const write = async (chunk: string) => {
  bytesWritten += Buffer.byteLength(chunk);
  // write() returning false means the internal buffer is full: wait for 'drain' instead of
  // queueing gigabytes in memory. This is the writing side of backpressure.
  if (!out.write(chunk)) await once(out, 'drain');
};

const startedAt = performance.now();
await write(csvLine(CSV_COLUMNS));
while (bytesWritten < targetBytes) {
  let chunk = '';
  for (let i = 0; i < ROWS_PER_CHUNK; i++) chunk += makeRow();
  rows += ROWS_PER_CHUNK;
  await write(chunk);
}
out.end();
await finished(out);

const seconds = ((performance.now() - startedAt) / 1000).toFixed(1);
console.log(
  `Wrote ${rows.toLocaleString()} rows (${(bytesWritten / 1024 / 1024).toFixed(0)} MB) to ${values.out} in ${seconds}s`,
);
