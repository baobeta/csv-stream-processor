/**
 * Compares CSV parsing approaches on the same file: speed, peak memory and correctness.
 * Each candidate runs in its own process so peak RSS is not shared between them.
 *
 *   pnpm compare:parsers data/transactions.csv
 *
 * Correctness is a fingerprint of the rows: how many records, the total length of the
 * `description` column, and how many descriptions contain a line break. Mature parsers should
 * agree; naive ones drift because the file has quoted commas and multi-line fields.
 */
import { execFileSync } from 'node:child_process';
import { createReadStream, readFileSync } from 'node:fs';
import { createInterface } from 'node:readline';
import { parse as csvParse } from 'csv-parse';
import csvParser from 'csv-parser';
import { parse as fastCsvParse } from 'fast-csv';
import Papa from 'papaparse';
import { importCsv } from '../src/import.ts';

interface Fingerprint {
  rows: number;
  descriptionChars: number;
  multiLineDescriptions: number;
}

type Row = Record<string, string | undefined>;

const CANDIDATES: Record<string, (file: string) => Promise<Fingerprint>> = {
  'csv-parse (used here)': async (file) => {
    const fp = emptyFingerprint();
    for await (const row of createReadStream(file).pipe(csvParse({ columns: true, bom: true }))) {
      addRow(fp, row);
    }
    return fp;
  },
  'csv-parser': async (file) => {
    const fp = emptyFingerprint();
    for await (const row of createReadStream(file).pipe(csvParser())) addRow(fp, row);
    return fp;
  },
  'fast-csv': async (file) => {
    const fp = emptyFingerprint();
    for await (const row of createReadStream(file).pipe(fastCsvParse({ headers: true }))) {
      addRow(fp, row);
    }
    return fp;
  },
  papaparse: (file) =>
    new Promise((resolve, reject) => {
      const fp = emptyFingerprint();
      Papa.parse<Row>(createReadStream(file), {
        header: true,
        skipEmptyLines: true,
        step: (result) => addRow(fp, result.data),
        complete: () => resolve(fp),
        error: reject,
      });
    }),
  'naive: readline + split(",")': async (file) => {
    const fp = emptyFingerprint();
    let header: string[] | undefined;
    for await (const line of createInterface({ input: createReadStream(file) })) {
      const cells = line.split(',');
      if (!header) {
        header = cells;
        continue;
      }
      addRow(fp, Object.fromEntries(header.map((name, i) => [name, cells[i]])));
    }
    return fp;
  },
  'naive: readFile + split': async (file) => {
    const fp = emptyFingerprint();
    const [headerLine = '', ...lines] = readFileSync(file, 'utf8').split('\n');
    const header = headerLine.split(',');
    for (const line of lines) {
      if (line === '') continue;
      const cells = line.split(',');
      addRow(fp, Object.fromEntries(header.map((name, i) => [name, cells[i]])));
    }
    return fp;
  },
  'this repo: csv-parse + zod validation (no DB)': async (file) => {
    const result = await importCsv({
      filePath: file,
      sink: { writeBatch() {}, close() {} },
      rejectsPath: '/dev/null',
      maxInvalidRows: Number.POSITIVE_INFINITY,
    });
    // Fingerprint is not tracked inside the pipeline; report the record count only.
    return { rows: result.processed, descriptionChars: -1, multiLineDescriptions: -1 };
  },
};

function emptyFingerprint(): Fingerprint {
  return { rows: 0, descriptionChars: 0, multiLineDescriptions: 0 };
}

function addRow(fp: Fingerprint, row: Row): void {
  const description = row.description ?? '';
  fp.rows++;
  fp.descriptionChars += description.length;
  if (description.includes('\n')) fp.multiLineDescriptions++;
}

const [file, only] = process.argv.slice(2);
if (!file) throw new Error('Usage: pnpm compare:parsers <file.csv>');

if (only) {
  // Child process: run a single candidate and report as JSON.
  const startedAt = performance.now();
  try {
    const fp = await CANDIDATES[only]!(file);
    const seconds = (performance.now() - startedAt) / 1000;
    const peakRssMb = process.resourceUsage().maxRSS / 1024;
    console.log(JSON.stringify({ ok: true, ...fp, seconds, peakRssMb }));
  } catch (error) {
    console.log(JSON.stringify({ ok: false, error: (error as Error).message.slice(0, 120) }));
  }
} else {
  console.log(
    '| approach | seconds | rows/s | peak RSS MB | rows | description chars | multi-line | correct |',
  );
  console.log('| --- | --- | --- | --- | --- | --- | --- | --- |');
  let reference: Fingerprint | undefined;
  for (const name of Object.keys(CANDIDATES)) {
    let out: string;
    try {
      out = execFileSync(
        process.execPath,
        ['--disable-warning=ExperimentalWarning', import.meta.filename, file, name],
        { encoding: 'utf8', maxBuffer: 1024 * 1024 },
      );
    } catch (error) {
      // A crash (e.g. out of memory) is a result too.
      const stderr = String((error as { stderr?: unknown }).stderr ?? '');
      const reason = stderr.match(/(RangeError|Error|FATAL)[^\n]*/)?.[0] ?? 'process crashed';
      console.log(`| ${name} | — | — | — | — | — | — | ❌ ${reason.slice(0, 80)} |`);
      continue;
    }
    const r = JSON.parse(out.trim().split('\n').at(-1)!);
    if (!r.ok) {
      console.log(`| ${name} | — | — | — | — | — | — | ❌ ${r.error} |`);
      continue;
    }
    reference ??= r;
    const fingerprinted = r.descriptionChars >= 0;
    const correct = fingerprinted
      ? r.rows === reference!.rows &&
        r.descriptionChars === reference!.descriptionChars &&
        r.multiLineDescriptions === reference!.multiLineDescriptions
      : r.rows === reference!.rows;
    console.log(
      `| ${name} | ${r.seconds.toFixed(1)} | ${Math.round(r.rows / r.seconds).toLocaleString()} | ` +
        `${Math.round(r.peakRssMb)} | ${r.rows.toLocaleString()} | ` +
        `${fingerprinted ? r.descriptionChars.toLocaleString() : 'n/a'} | ` +
        `${fingerprinted ? r.multiLineDescriptions.toLocaleString() : 'n/a'} | ${correct ? '✅' : '❌'} |`,
    );
  }
}
