# How this importer compares, and why it is built this way

This document answers two questions:

1. **Is this approach actually good?** We tested it against the tools and techniques people commonly use for the same job.
2. **Why was each design decision made?** Every choice has a cost. This document says what we gave up, not just what we gained.

[Part 1](#part-1-the-short-version) is written for everyone, no programming knowledge needed. [Part 2](#part-2-for-engineers) has the measurements, methodology, and one decision record per design choice.

---

## Part 1: The short version

### What the project does

Companies regularly receive huge spreadsheet-like files (CSV files) — bank transactions, orders, customer lists — and need to load them into a database. A 2 GB file can hold 16 million rows.

The difficulty is not the size alone. It is that:

- **The file is bigger than the computer's working memory** wants to handle at once, so it has to be processed piece by piece, like moving a library through a single door one box at a time.
- **Real files contain mistakes**: an email without an `@`, a date like the 45th day of the 13th month, a stray quotation mark. A good importer must notice these, set them aside for someone to fix, and keep going.
- **Imports get interrupted**: a laptop goes to sleep, a server restarts. Starting a two-hour import from zero is painful; the importer should continue from where it stopped, without loading anything twice.

### What we compared it against

We ran the same 300 MB file (2.456 million rows, about 0.1% of them deliberately broken) through:

- **Four popular open-source libraries** that read CSV files in Node.js: `papaparse`, `csv-parser`, `fast-csv`, and `csv-parse` (the one this project uses).
- **Two "quick and simple" approaches** that many tutorials show.
- **Four ways of loading into a Postgres database**, from the simplest (one row at a time) to Postgres's own built-in bulk loader, the fastest possible baseline.

Each test measured three things: **speed**, **memory used**, and, most importantly, **whether the result was correct**.

### What we found

| Question                                      | Answer                                                                                                                                                                                                                                                                                      |
| --------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Is it the fastest at reading files?           | **No.** One library (`papaparse`) reads the file about 3.5× faster. We chose a slower one on purpose, for reasons explained below.                                                                                                                                                          |
| Do the "quick and simple" approaches work?    | **No.** They finished fast but produced the wrong answer: they counted 2.95 million rows instead of 2.456 million, because they split text that legitimately contains line breaks. One of them also used 11× more memory than the library we chose (1.15 GB instead of 104 MB).             |
| Does any library lose data silently?          | **Yes.** Given a file with one stray quotation mark, `csv-parser` returned 2 rows out of 20,000 **without reporting any error**. Our choice stops and reports the exact line.                                                                                                               |
| How does it compare to Postgres's own loader? | Postgres's built-in loader was **as fast to 2× faster**, depending on the test session. But by default it **refuses the entire file** if a single row is broken, and it cannot resume. Ours loads every good row, sets aside the bad ones with the reason, and resumes after interruptions. |
| What about the way most tutorials do it?      | Inserting one row at a time would take roughly **1.9 hours** for this file. Ours took about a minute or less.                                                                                                                                                                               |
| Did testing change the project?               | **Yes.** Benchmarking exposed that our database writer was 2.5× slower than it needed to be. We fixed it; the comparison above uses the fixed version.                                                                                                                                      |

### What happens with a file that contains mistakes

This is where the approaches differ most. With the same file containing 2,500 broken rows:

| Approach                                                                       | Outcome                                                                                                                                                                                        |
| ------------------------------------------------------------------------------ | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Postgres built-in loader                                                       | ❌ Stops at the first bad date. **Nothing** is loaded.                                                                                                                                         |
| Postgres built-in loader, "ignore errors" setting (skips rows it cannot store) | ⚠️ Finishes fast, but only skips the 624 rows with impossible dates and reports just a count. **1,876 bad rows get into the database** (invalid emails, unknown currencies, negative amounts). |
| Batched inserts (rows sent in groups of 1,000)                                 | ❌ Stops at the first bad date, **leaving 8,000 rows in a half-filled table**.                                                                                                                 |
| One row at a time                                                              | ❌ Stops at the first bad date, also leaving a half-filled table with no record of where it stopped.                                                                                           |
| **This project**                                                               | ✅ Loads all 2,453,500 good rows. Writes all 2,500 bad rows to a separate file with line number and reason.                                                                                    |

### What happens when an import is interrupted or run twice

When loading into Postgres, the importer writes down how far it got **in the same step** as the rows themselves, so after a crash or a restart it continues exactly where it stopped and never loads a group of rows twice. If someone starts the same file a second time while the first import is still running, the second one stops immediately with a clear message instead of competing with the first. A server asking the importer to stop (as happens during a deployment) is handled like pressing Ctrl+C: rows already saved are kept, and the next run continues from there.

### The bottom line

This project is **not the fastest tool at any single step**, and it does not claim to be. It is designed to be **correct, safe with messy real-world data, and able to recover from interruptions**, while staying between level with and 2× slower than the fastest possible baseline, and keeping memory flat no matter how large the file is.

**Use something else when** the file is guaranteed clean and a failed import costs nothing to retry: Postgres's built-in loader is simpler and faster for that. **Use this approach when** the data comes from outside your control, the file is large, and losing or duplicating rows matters.

---

## Part 2: For engineers

### Methodology

| Aspect      | Setup                                                                                                                                                                      |
| ----------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Data        | Generated by `pnpm generate`: 300 MB, 2,456,000 rows, UUIDv4 keys. ~20% of rows have a multi-line quoted field; ~20% have a quoted comma. "Dirty" file: 0.1% invalid rows. |
| Machine     | MacBook (Apple Silicon), Node.js 24.11, Postgres 17 in Docker Desktop.                                                                                                     |
| Isolation   | Each parser and each loader runs in its **own process**, so peak RSS is not shared.                                                                                        |
| Correctness | Parsers: fingerprint of record count, total `description` length, and count of multi-line descriptions. Loaders: row count and distinct id count in the database.          |
| Repeats     | Loader comparisons were run twice, back to back, with a `psql \copy` baseline in the same session.                                                                         |
| Scripts     | [`scripts/compare-parsers.ts`](../scripts/compare-parsers.ts), [`scripts/compare-loaders.ts`](../scripts/compare-loaders.ts), [`scripts/bench.ts`](../scripts/bench.ts).   |

> **Read ratios, not absolute seconds.** Docker Desktop's disk speed drifted by about 2.5× between sessions: the same `psql \copy` took 12.5 s in one session and 30 s in another. Only numbers measured in the same session are compared with each other below.

### Results: parsing

Parse only, no database. Same file for every row.

| Approach                                | Time   | Rows/s | Peak RSS | Records   | Correct |
| --------------------------------------- | ------ | ------ | -------- | --------- | ------- |
| `papaparse` 5.7                         | 3.6 s  | 690k   | 127 MB   | 2,456,000 | ✅      |
| `csv-parser` 3.2                        | 9.2 s  | 266k   | 112 MB   | 2,456,000 | ✅      |
| **`csv-parse` 7.0 (used here)**         | 12.8 s | 193k   | 104 MB   | 2,456,000 | ✅      |
| `fast-csv` 5.0                          | 20.7 s | 119k   | 199 MB   | 2,456,000 | ✅      |
| `readline` + `split(',')`               | 2.7 s  | —      | 126 MB   | 2,947,152 | ❌      |
| `readFile` + `split`                    | 3.3 s  | —      | 1,154 MB | 2,947,152 | ❌      |
| This repo's pipeline: `csv-parse` + zod | 22.0 s | 112k   | 340 MB   | 2,456,000 | ✅      |

All four libraries agree with each other on every fingerprint (50,085,290 description characters, 491,152 multi-line descriptions). The naive approaches count every embedded line break as a new row (2,456,000 + 491,152 = 2,947,152). `readFile` also holds the whole file as one string; V8 caps a string at 0x1fffffe8 characters (~512 MB), and reading a 600 MB file this way threw `ERR_STRING_TOO_LONG`, so it cannot open the 2 GB target file at all.

### Results: malformed input

Three small files fed to each library with default or strict settings.

| Malformed file                    | `csv-parse` (here)      | `csv-parser`                    | `fast-csv`             | `papaparse`                                |
| --------------------------------- | ----------------------- | ------------------------------- | ---------------------- | ------------------------------------------ |
| Unclosed quote on row 2 of 20,000 | ✅ Throws at line 1012  | ❌ **Returns 2 rows, no error** | ✅ Throws              | ⚠️ Returns 2 rows, reports `MissingQuotes` |
| Row with an extra column          | ✅ Throws, names line 3 | ✅ Throws (with `strict`)       | ✅ Throws              | ⚠️ Continues, reports `TooManyFields`      |
| Row with missing columns          | ✅ Throws, names line 3 | ✅ Throws (with `strict`)       | ❌ Returns it silently | ⚠️ Continues, reports `TooFewFields`       |

`papaparse` is lenient by design: it keeps going and attaches errors to each result, so correctness depends on every caller checking `result.errors`. `csv-parser` without a size guard silently swallowed 19,998 rows into one field.

### Results: loading into Postgres

Same session, run twice. `psql \copy` is the ceiling: Postgres's native C loader with no validation.

| Approach                                             | Clean file (run 1 / run 2)                           | × `psql` | Dirty file (0.1% invalid)                                         | Resumable |
| ---------------------------------------------------- | ---------------------------------------------------- | -------- | ----------------------------------------------------------------- | --------- |
| `psql \copy`                                         | 30.2 s / 28.7 s                                      | 1.0×     | ❌ Aborts on first bad date; 0 rows loaded                        | No        |
| **This repo** (validate → batch → COPY)              | 53.6 s / 59.9 s                                      | ~1.9×    | ✅ 40.5 s / 45.1 s; 2,453,500 loaded, 2,500 rejected with reasons | Yes       |
| Multi-row `INSERT`, 1000 rows/statement (autocommit) | 77.8 s / 155.5 s                                     | 2.6–5.4× | ❌ Aborts on first bad date; 8,000 rows left in the table         | No        |
| One `INSERT` per row (autocommit)                    | ~2.8 ms/row → ≈1.9 h (extrapolated from 50,000 rows) | ≫100×    | ❌ Aborts; table left partially filled                            | No        |

Two more measurements, from later sessions:

- **Postgres 17 `COPY ... ON_ERROR ignore`** on the dirty file: 20.4 s. It skipped the 624 rows whose values do not fit the column type (impossible dates) and printed only `NOTICE: 624 rows were skipped`. The other **1,876 invalid rows were loaded**: 618 emails without `@`, 611 `XXX` currencies, 647 negative amounts. Column types cannot express those rules. This repo rejected all 2,500 (624 + 618 + 611 + 647) with line numbers and reasons.
- **After the sink fix**, in a session where `psql \copy` took 36.2 s, this repo took **37.4 s** on the dirty file, level with the native loader while validating every row. With database and Node working in parallel, the ratio to `psql` ranged from ~1.0× to ~1.9× across sessions.

Peak RSS: this repo 244–271 MB, multi-row `INSERT` 225–235 MB (same session). Resume was verified end to end with this sink: Ctrl+C at 700,000 rows (database and checkpoint agree), resume, final 2,453,500 rows with 2,453,500 distinct ids and 2,500 non-duplicated rejects.

### A bug the benchmark found

The first version of the Postgres sink sent every batch through a temp table: `COPY` into the temp table, then `INSERT ... SELECT ... ON CONFLICT DO NOTHING` into the real table. It was correct but slow and noisy (57–144 s across runs on UUIDv4 files), slower than plain multi-row `INSERT`. Instrumenting each phase over one run (~125 s of database time):

| Phase                               | Time  | Likely cause                                                             |
| ----------------------------------- | ----- | ------------------------------------------------------------------------ |
| `COPY` into the temp table          | ~47 s | Writing every row twice: once into the temp table, once into the target  |
| `INSERT ... SELECT ... ON CONFLICT` | ~56 s | `ON CONFLICT` uses speculative insertion per row, even with no conflicts |
| `COMMIT`                            | ~17 s | `ON COMMIT DELETE ROWS` truncates the temp table on every commit         |

Three variants measured back to back (`psql \copy` = 32.6 s in that session):

| Variant                                                                           | Time       |
| --------------------------------------------------------------------------------- | ---------- |
| A. Always merge through the temp table (original)                                 | 126.1 s    |
| B. Same, but one stream chunk per batch instead of one per row                    | 118.3 s    |
| **C. `COPY` straight into the target inside a savepoint; merge only on conflict** | **49.4 s** |

My first guess (variant B, chunking) explained only 6%. The real cost was the unconditional merge. Variant C is now the sink. Its worst case, re-importing a file whose every row already exists so every batch falls back to the merge, took 64.7 s versus 59.2 s for the first load.

### Decision records

Each record follows the same shape: **decision**, **why**, **alternatives**, **tradeoff accepted**, and **when to choose differently**.

#### 1. Stream the file instead of reading it whole

- **Decision.** `fs.createReadStream` with 1 MiB chunks, wired with `stream/promises.pipeline()`.
- **Why.** Memory stays flat regardless of file size (peak heap 70–120 MB for every configuration). `readFile` used 1.15 GB for 300 MB and cannot open a 2 GB file at all, because a string is capped at ~512 MB. `pipeline()` also propagates errors and destroys every stage, so no file descriptors leak.
- **Alternatives.** `readFile` (simple, bounded by RAM and the string limit); `readline` (streams, but splits on line breaks that live inside quoted fields).
- **Tradeoff.** More moving parts than `readFile`: asynchronous stages, backpressure, error propagation to reason about.
- **Choose differently when** files are guaranteed small (a few MB). Then `readFile` + a parser is simpler and fine.

#### 2. `csv-parse` instead of the faster `papaparse`

- **Decision.** `csv-parse`, with `max_record_size` set to 64 KB.
- **Why.** Three properties the rest of the design depends on, which `papaparse` does not provide out of the box:
  - **Fails loudly.** It throws on an unclosed quote or a wrong column count, naming the line. `papaparse` continues and records an error on the result; `csv-parser` without a size guard silently lost 19,998 of 20,000 rows.
  - **Byte offsets.** `info.bytes_records` is an exact byte offset, which resume needs for `createReadStream({ start })`. `papaparse`'s `meta.cursor` counts characters: on a 38-byte file containing Vietnamese text it reported 32, which would resume at the wrong position.
  - **Line numbers.** `info.lines` gives the physical line of each record, used in `rejects.ndjson`.
- **Alternatives.** `papaparse` (3.5× faster; lenient; character cursor). `csv-parser` (1.4× faster; silent data loss without a guard). `fast-csv` (slower, more memory, silently accepted a short row).
- **Tradeoff.** Parsing is 3.5× slower than the fastest library. In a database import this costs little, because parsing is about 13 s of a ~55 s Postgres load. It costs more for SQLite with UUIDv7, where the database is fast and parsing becomes most of the runtime.
- **Choose differently when** the input is trusted, resume is not needed, and parse speed dominates. Then `papaparse` with a strict check of `result.errors` on every row is a reasonable trade.

#### 3. Async generators as pipeline stages

- **Decision.** Validation and batching are `async function*` stages passed to `pipeline()`, not `Transform` subclasses.
- **Why.** Generators are pull-based: a stage produces the next item only when the next stage asks for it, so backpressure needs no manual `push()`/callback handling. It also makes checkpoints exact: when a batch reaches the sink, the validation stage is paused on the batch's last row, so the recorded byte offset is the end of that batch.
- **Alternatives.** `Transform` streams in object mode (the classic approach; more boilerplate, easier to get backpressure wrong).
- **Tradeoff.** A subtle failure mode: when a generator throws inside `for await` over a stream, the default iterator destroys the stream with a generic `AbortError` that can reach `pipeline()` first and hide the real error. Fixed with `source.iterator({ destroyOnReturn: false })` and covered by a regression test.
- **Choose differently when** the team is more familiar with classic streams, or stages need fine control over buffering.

#### 4. Validate in the application, not only in the database

- **Decision.** A zod schema turns each text row into typed data before it reaches the database.
- **Why.** The database rejects bad data too, but it rejects the **whole statement or batch** and gives a single error. Validating per row lets the import keep every good row and report each bad one with a reason. It also checks rules the database column types do not (email format, allowed currencies).
- **Alternatives.** Rely on database column types and constraints alone (what `psql \copy` and the `INSERT` approaches do). Postgres 17's `COPY ... ON_ERROR ignore` skips rows with type errors and reports only a count by default; in our test it let 1,876 invalid rows through because their values fit the column types. `CHECK` constraints could express more rules, but a violation aborts the whole `COPY`.
- **Tradeoff.** CPU cost: the pipeline goes from 12.8 s (parse only) to 22.0 s with validation, and peak RSS from 104 MB to 340 MB. Rules now live in two places (schema and table definition) and must be kept consistent.
- **Choose differently when** data is produced by a trusted system you control. Then database constraints alone are enough.

#### 5. Reject and continue, with a limit

- **Decision.** Invalid rows go to `rejects.ndjson` with line number and reason; the import aborts once `--max-invalid` (default 1000) is exceeded. The row is written before the abort, so the row that crossed the limit is not lost.
- **Why.** A single bad row should not throw away 2.4 million good ones, but a file where everything is wrong (a wrong column order, a different file format) should stop early instead of producing two million rejects.
- **Alternatives.** All-or-nothing (database-native behaviour). Skip silently (fast, but data disappears without a trace).
- **Tradeoff.** The database ends up with a partial file by design; someone must look at the rejects file. The limit is absolute, not a percentage, so it must be tuned to file size (it also counts across resumed runs).
- **Choose differently when** the data must be loaded completely or not at all, such as accounting periods. Use `--max-invalid 0`, which fails on the first invalid row.

#### 6. Batch size 5000

- **Decision.** One transaction per 5000 rows.
- **Why.** Measured, not guessed. For Postgres the curve is U-shaped: with the current sink, batch 100 took 136.5 s, 1000 took 52.3 s, 5000 took 37.4 s, and 20000 took 44.2 s (peak heap 107–112 MB up to 5000 rows, 145 MB at 20000). Tiny batches pay a fixed round-trip cost per batch; huge ones grow memory and slow each transaction. SQLite barely cares (21–22 s at every size; in-process, no network).
- **Alternatives.** Row-by-row (≈1.9 h for this file); one huge transaction (lowest overhead, but a crash loses everything and memory grows with the batch).
- **Tradeoff.** Up to one batch of work is redone after a crash.
- **Choose differently when** the database is remote with high latency (larger batches) or memory is tight (smaller batches). Run `pnpm bench` with `--batch-sizes` on your own setup.

#### 7. Postgres: `COPY` first, merge only when needed

- **Decision.** Each batch is `COPY`ed straight into the target table inside a savepoint. Only if that hits a unique violation (a resumed or repeated import) is the batch rolled back to the savepoint and merged through a temp table with `INSERT ... ON CONFLICT DO NOTHING`.
- **Why.** `COPY` is the fastest bulk path in Postgres, but it cannot skip duplicates. Duplicates are rare in practice: they only happen when replaying a batch after a crash or re-importing a file. So the common case should pay nothing for them. This made the sink 2.5× faster than always merging (49 s vs 126 s in the same session), and brought the whole import level with `psql \copy` in a later session (37.4 s vs 36.2 s).
- **Alternatives.** Always merge through a temp table (simple, consistently slow). Multi-row `INSERT ... ON CONFLICT` (no temp table, but parameter limits and slower). `psql \copy` (fastest, no validation, no resume).
- **Tradeoff.** More complex code with two paths, and a batch that conflicts does its `COPY` twice. That worst case was measured at ~9% slower overall.
- **Choose differently when** most batches are expected to conflict (for example, frequent full re-syncs of the same data). Then always merging is simpler and about as fast.

#### 8. SQLite: index-free staging, or time-ordered keys

- **Decision.** Two SQLite strategies: `direct` (insert into the indexed table) and `staging` (append to an index-free table, merge once in key order at the end). The generator can produce UUIDv7 keys.
- **Why.** Random UUIDv4 primary keys land on random B-tree pages; once the index outgrows the page cache, nearly every insert touches disk. On the same file: direct 122 s, staging 21 s, direct with UUIDv7 17 s.
- **Alternatives.** Drop the index and rebuild it after loading (fast, but the table is unprotected against duplicates while loading). A bigger cache only helped partly (344 s → 122 s).
- **Tradeoff.** Staging makes rows visible only at the end, and needs disk space for a second copy. UUIDv7 leaks creation time in the id.
- **Choose differently when** you control key generation: prefer UUIDv7 or integer keys and use `direct`.

#### 9. Idempotent writes keyed on the primary key

- **Decision.** Every write ignores rows whose id is already stored.
- **Why.** It makes replays harmless: re-running an import, or redoing one batch after a crash, never creates duplicates. This is what lets the checkpoint be simple.
- **Alternatives.** Exactly-once bookkeeping without idempotency (harder; see the next decision).
- **Tradeoff.** Requires a natural unique key in the data. A row whose id exists is skipped, not updated, so corrections must be imported differently (an upsert would be a different design).
- **Choose differently when** re-imports should update existing rows. Use `ON CONFLICT DO UPDATE` and think about which version wins.

#### 10. Checkpoint in a file after each commit

- **Decision.** After each committed batch, write `{byte offset, line, header, counters, rejects size}` to a JSON file, via a temp file and an atomic `rename`. Resume only when the file path, size, mtime and destination all match.
- **Why.** Saving after the commit gives at-least-once delivery, and idempotent writes turn that into effectively-once. A crash between commit and checkpoint replays one batch, which the sink absorbs. The file identity check prevents applying an offset to a different or edited file.
- **Alternatives.** Store the checkpoint in the database within the same transaction (true exactly-once, no idempotency needed, but couples the importer to the destination's schema).
- **Tradeoff.** A replayed batch is re-validated and re-written. The checkpoint file lives next to the data, so it must not be shared between machines.
- **Choose differently when** the destination cannot be made idempotent. Then put the checkpoint in the database transaction.
- **Superseded for Postgres** by [decision 14](#14-checkpoint-inside-the-database-transaction). SQLite still uses the file.

#### 11. Truncate the rejects file on resume

- **Decision.** The checkpoint records the rejects file's size; on resume the file is truncated back to it. Rejects are written synchronously.
- **Why.** Found by a test: invalid rows after the last checkpoint were rejected once by the interrupted run and again after resume, duplicating lines. The database was protected by idempotency; the rejects file was not. Synchronous writes keep the size on disk in step with the checkpoint.
- **Alternatives.** Deduplicate rejects afterwards (extra pass); key rejects by line number in a small database.
- **Tradeoff.** Synchronous I/O on the main thread for each invalid row. Fine at 0.1%; it would matter if a large share of rows were invalid, but then the `--max-invalid` limit stops the import anyway.
- **Choose differently when** rejects must go to a remote system (a queue or table). Then make that destination idempotent too.

#### 12. Money as integer minor units

- **Decision.** `"12.50"` becomes `1250` by splitting the string, never by multiplying a float.
- **Why.** `Number('0.29') * 100 === 28.999999999999996`. Floats cannot represent most decimal amounts exactly.
- **Alternatives.** `numeric` columns with string values end to end; a decimal library.
- **Tradeoff.** Assumes two decimal places, which is wrong for currencies like JPY (0) or KWD (3).
- **Choose differently when** handling many currencies: store the currency's exponent, or keep `numeric`.

#### 13. Run TypeScript directly on Node 24

- **Decision.** No build step; Node 24 strips types at runtime. `erasableSyntaxOnly` enforces syntax that can simply be removed.
- **Why.** Fewer tools to install and configure for a learning project; the code you read is the code that runs.
- **Alternatives.** `tsc` build, or `tsx`.
- **Tradeoff.** No enums, namespaces or constructor parameter properties. `node:sqlite` is still experimental in Node 24 (its warning is silenced in the scripts).
- **Choose differently when** you need to support older Node versions or publish a library: add a build step.

#### 14. Checkpoint inside the database transaction

- **Decision.** Sinks may provide their own checkpoint store. The Postgres sink keeps the checkpoint as `jsonb` in an `import_checkpoints` table, keyed on `<table>:<absolute file path>`, and upserts it in the same transaction as each batch. The row is deleted when the file is complete. `--checkpoint` is now only used by SQLite.
- **Why.** The checkpoint file of decision 10 is a second write after the commit. A crash between the two replays one batch on resume. `ON CONFLICT DO NOTHING` hid that replay, so it never showed up as a wrong count, but it was still extra work and depended on idempotency for correctness. Committing rows and checkpoint together means they cannot disagree: a resumed import starts exactly after the last committed batch.
- **Alternatives.** Keep the file and rely on idempotency (decision 10). Write the checkpoint to a separate store, such as Redis (still two writes, same gap).
- **Tradeoff.** One small upsert per batch, about 490 for the 2.456-million-row file. The importer creates `import_checkpoints` itself with `CREATE TABLE IF NOT EXISTS`, so it needs `CREATE` privilege on the schema, and two importers starting at once could race on that DDL; it therefore runs behind a global transaction-level advisory lock. `ON CONFLICT DO NOTHING` is kept for re-imports of the same file, so duplicate ids are still skipped rather than updated, and `written` counts rows handed to the sink, not rows inserted.
- **Measured.** Interleaved A/B against the previous version (`master`), 300 MB file, `--fresh`, table truncated before each run, run in the order master, branch, master, branch. The machine was heavily loaded by unrelated work (load average 14–30):

  | Version                    | Run 1   | Run 2   | Mean    |
  | -------------------------- | ------- | ------- | ------- |
  | Previous (file checkpoint) | 47.7 s  | 138.7 s | 93.2 s  |
  | Checkpoint in transaction  | 184.9 s | 34.3 s  | 109.6 s |

  Both versions loaded 2,453,550 rows in every run (this file had 2,450 invalid rows, hence 2,453,550 rather than the 2,453,500 of the older runs above). The spread within each version (91 s and 151 s) is far larger than the 16 s difference between the means, so no large regression was detected; because the machine was heavily loaded, a small difference cannot be ruled out. Two earlier runs of the new version in the same session took 89.4 s and 61.7 s. Re-measure on an idle machine before quoting absolute numbers.

- **Choose differently when** the importer must not create tables or own any schema in the destination. Then keep the file checkpoint with an idempotent sink, or create `import_checkpoints` through your migration tool.

#### 15. One import per file, retries for transient conflicts, and a bounded shutdown

- **Decision.** Three related rules for running the importer as a job:
  - **Lock.** Before reading the checkpoint, the Postgres sink takes a session-level advisory lock (`pg_try_advisory_lock`) on `<table>:<absolute file path>`. If another session holds it, the import fails with `ImportInProgressError` and exit code 75 (`EX_TEMPFAIL`).
  - **Retry.** A batch rolled back with `40001` (serialization failure) or `40P01` (deadlock) is retried, up to 3 attempts in total, with exponential backoff and full jitter (base 100 ms). Other errors, and any error where `ROLLBACK` itself failed, are not retried.
  - **Shutdown.** `SIGTERM` is handled like Ctrl+C: stop reading, keep committed batches, exit 143 (Ctrl+C: 130). If shutdown takes longer than 10 s, the process prints a message and exits anyway.
- **Why.** Two workers resuming from the same checkpoint would both write the same batches and both truncate and append to the rejects file. Locking before the checkpoint is read prevents that. A session-level lock is released by Postgres if the process dies, so a crash never leaves a stale lock. Serialization failures and deadlocks are transient by definition: Postgres has rolled the transaction back and the same batch can simply run again. Container platforms stop jobs with `SIGTERM`, not `SIGINT`, so without the handler a deployment would kill the import mid-batch.
- **Alternatives.** A lock row in a table (survives crashes, so needs expiry and cleanup). No lock, relying on idempotency (correct rows, but duplicated work and a corrupted rejects file). Retrying lost connections in-process (would need to re-take the lock and re-create the temp table on a new session).
- **Tradeoff.**
  - A lost connection is not retried in-process, because the lock and the temp table die with the session. The import fails, and running it again resumes from the checkpoint, so the job runner must retry.
  - If the connection drops mid-import, the lock is gone while this worker may still append rejects for up to one batch before its next write fails. Concurrent workers must not share a rejects path.
  - 10 s is a guess at "a batch normally commits within a second" with margin. It is deliberately below Kubernetes' default 30 s `terminationGracePeriodSeconds`, so the importer exits on its own before `SIGKILL`. A larger batch size or a slow database may need a longer grace period on both sides.
  - The lock is per file path and table. The same content under a different path is not detected.
- **Choose differently when** imports are scheduled by a queue that already guarantees one consumer per file. Then the lock is a cheap second guard, not the main one.

### Where other tools are the better choice

| Situation                                                    | Better choice                                                           |
| ------------------------------------------------------------ | ----------------------------------------------------------------------- |
| Clean, trusted file; retrying a failed load is cheap         | `psql \copy` or `COPY FROM`: as fast or faster, and nothing to maintain |
| Only type errors matter, and a skipped-row count is enough   | Postgres 17 `COPY ... ON_ERROR ignore`                                  |
| Parse speed matters most, input is trusted, no resume needed | `papaparse` with strict checking of `result.errors`                     |
| Parsing CSV in the browser                                   | `papaparse` (designed for it, with web workers)                         |
| Transforming files without loading a database                | DuckDB or similar columnar tools: far faster for analytical reads       |
| Large recurring data pipelines across many sources           | A dedicated loader (pgloader, an ETL service) rather than custom code   |

### Reproducing the results

```bash
pnpm install
cp .env.example .env && pnpm db:up
pnpm generate --size-mb 300 --out data/dirty.csv
pnpm generate --size-mb 300 --out data/clean.csv --invalid-ratio 0

pnpm compare:parsers data/dirty.csv                                   # parsing table
node --env-file=.env scripts/compare-loaders.ts this-repo data/clean.csv  # one loader per process
node --env-file=.env scripts/compare-loaders.ts batched-insert data/clean.csv
node --env-file=.env scripts/compare-loaders.ts row-insert data/clean.csv 50000
pnpm bench data/dirty.csv --targets sqlite-direct,sqlite-staging,postgres --batch-sizes 1000,5000
```

For the `psql \copy` baseline, create a table with the CSV's columns (`amount numeric(14,2)`) and pipe the file in:

```bash
docker compose exec -T postgres psql -U csv -d csv_import \
  -c "\copy compare_copy FROM STDIN WITH (FORMAT csv, HEADER true)" < data/clean.csv
```

Always measure the baseline in the same session as the approach you compare it with.
