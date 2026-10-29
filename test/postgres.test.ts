import pg from 'pg';
import { afterAll, afterEach, beforeEach, describe, expect, it } from 'vitest';
import type { Checkpoint } from '../src/checkpoint.ts';
import { PostgresSink } from '../src/sinks/postgres.ts';
import { ImportInProgressError } from '../src/sinks/types.ts';
import { tx } from './helpers.ts';

const databaseUrl = process.env.DATABASE_URL;
// Locally these tests skip without a database; in CI a skip would hide a broken setup.
if (process.env.CI && !databaseUrl) throw new Error('DATABASE_URL must be set in CI');
const table = `test_transactions_${process.pid}`;
const FILE = '/data/a.csv';
const checkpointAt = (written: number): Checkpoint => ({
  filePath: FILE,
  fileSize: 1_000,
  fileMtimeMs: 1_700_000_000_000.5,
  target: 'test',
  columns: ['id', 'amount'],
  bytes: written * 10,
  lines: written + 1,
  rejectsBytes: 0,
  counters: { processed: written, invalid: 0, written },
});

/** Drops a test table and any checkpoints stored for it. */
const dropTable = async (name: string): Promise<void> => {
  const client = new pg.Client({ connectionString: databaseUrl });
  await client.connect();
  await client.query(`DROP TABLE IF EXISTS ${name}`);
  await client.query(`DELETE FROM import_checkpoints WHERE key LIKE $1`, [`${name}:%`]);
  await client.end();
};

// Runs only when a database is configured (`pnpm db:up` + .env).
describe.skipIf(!databaseUrl)('PostgresSink', () => {
  afterAll(async () => {
    const client = new pg.Client({ connectionString: databaseUrl });
    await client.connect();
    await client.query(`DROP TABLE IF EXISTS ${table}`);
    await client.end();
  });

  it('COPYs batches, skips duplicate ids and keeps empty strings non-null', async () => {
    const sink = new PostgresSink(databaseUrl!, { table });
    await sink.open();
    try {
      await sink.writeBatch([
        tx(1, { description: 'Book "DDIA", 2nd\nedition' }),
        tx(2, { description: '' }),
      ]);
      await sink.writeBatch([tx(2), tx(3)]);
      expect(await sink.count()).toBe(3);
      // First batch is new (fast path); second contains the stored id 2 (merge path).
      expect(sink.stats).toEqual({ directBatches: 1, mergedBatches: 1 });
    } finally {
      await sink.close();
    }

    const client = new pg.Client({ connectionString: databaseUrl });
    await client.connect();
    const { rows } = await client.query(
      `SELECT description, amount_minor::int AS amount FROM ${table} ORDER BY id`,
    );
    await client.end();
    expect(rows).toEqual([
      { description: 'Book "DDIA", 2nd\nedition', amount: 100 },
      { description: '', amount: 100 },
      { description: 'Coffee', amount: 100 },
    ]);
  });

  it('falls back to the merge for a duplicate id inside one batch, keeping the batch atomic', async () => {
    const dupTable = `${table}_dup`;
    const sink = new PostgresSink(databaseUrl!, { table: dupTable });
    await sink.open();
    try {
      await sink.writeBatch([tx(10), tx(11), tx(10)]);
      expect(await sink.count()).toBe(2);
      expect(sink.stats).toEqual({ directBatches: 0, mergedBatches: 1 });
    } finally {
      await sink.close();
      const client = new pg.Client({ connectionString: databaseUrl });
      await client.connect();
      await client.query(`DROP TABLE IF EXISTS ${dupTable}`);
      await client.end();
    }
  });

  it('commits the checkpoint in the same transaction as the batch', async () => {
    const ckptTable = `${table}_ckpt`;
    const sink = new PostgresSink(databaseUrl!, { table: ckptTable });
    try {
      expect(await sink.checkpoints.load(FILE)).toBeUndefined();
      await sink.open();
      await sink.writeBatch([tx(1)], checkpointAt(1));
      expect(await sink.checkpoints.load(FILE)).toEqual(checkpointAt(1));

      // 'TOOLONG' overflows char(3), so the COPY fails: neither the row nor the newer checkpoint lands.
      await expect(
        sink.writeBatch([tx(2, { currency: 'TOOLONG' as 'USD' })], checkpointAt(2)),
      ).rejects.toThrow(/too long/);
      expect(await sink.checkpoints.load(FILE)).toEqual(checkpointAt(1));
      expect(await sink.count()).toBe(1);

      // The COPY succeeds but jsonb rejects \u0000, so the upsert fails: the row must roll back too.
      await expect(
        sink.writeBatch([tx(3)], { ...checkpointAt(3), target: 'bad\u0000' }),
      ).rejects.toThrow(/unsupported Unicode escape sequence/);
      expect(await sink.checkpoints.load(FILE)).toEqual(checkpointAt(1));
      expect(await sink.count()).toBe(1);

      // A batch with an already-stored id goes through the merge and still commits its checkpoint.
      await sink.writeBatch([tx(1), tx(4)], checkpointAt(2));
      expect(await sink.checkpoints.load(FILE)).toEqual(checkpointAt(2));
      expect(await sink.count()).toBe(2);
      expect(sink.stats).toEqual({ directBatches: 1, mergedBatches: 1 });

      await sink.checkpoints.clear(FILE);
      expect(await sink.checkpoints.load(FILE)).toBeUndefined();
    } finally {
      await sink.close();
      await dropTable(ckptTable);
    }
  });

  it('creates the tables safely when several sinks open at once', async () => {
    // One round rarely loses the race, so try a few fresh tables.
    for (let round = 0; round < 5; round++) {
      const raceTable = `${table}_race${round}`;
      const sinks = Array.from(
        { length: 4 },
        () => new PostgresSink(databaseUrl!, { table: raceTable }),
      );
      try {
        await expect(Promise.all(sinks.map((s) => s.open()))).resolves.toBeDefined();
      } finally {
        await Promise.all(sinks.map((s) => s.close()));
        await dropTable(raceTable);
      }
    }
  });

  it('lets only one import of a file run at a time', async () => {
    const first = new PostgresSink(databaseUrl!, { table });
    const second = new PostgresSink(databaseUrl!, { table });
    try {
      await first.lock(FILE);
      await expect(second.lock(FILE)).rejects.toBeInstanceOf(ImportInProgressError);
      await second.lock('/data/other.csv');
    } finally {
      await first.close();
      await second.close();
    }

    // Closing the connection released the lock.
    const third = new PostgresSink(databaseUrl!, { table });
    try {
      await third.lock(FILE);
    } finally {
      await third.close();
    }
  });

  it('survives the server dropping the connection and fails the next query instead', async () => {
    const sink = new PostgresSink(databaseUrl!, { table });
    const admin = new pg.Client({ connectionString: databaseUrl });
    await admin.connect();
    try {
      await sink.lock(FILE);
      // Find only this sink's backend: the holder of its advisory lock. A bigint advisory key is
      // split into classid (high 32 bits) and objid (low 32 bits), both unsigned oids, so rebuild
      // it as (classid << 32) | objid in bigint arithmetic; this also holds for negative hashes.
      const { rows } = await admin.query<{ pid: number }>(
        `SELECT pid FROM pg_locks
         WHERE locktype = 'advisory' AND objsubid = 1 AND granted
           AND ((classid::bigint << 32) | objid::bigint) = hashtextextended($1, 0)`,
        [`${table}:${FILE}`],
      );
      expect(rows).toHaveLength(1);
      const pid = rows[0]!.pid;
      await admin.query('SELECT pg_terminate_backend($1)', [pid]);
      for (let i = 0; i < 100; i++) {
        const { rowCount } = await admin.query('SELECT 1 FROM pg_stat_activity WHERE pid = $1', [
          pid,
        ]);
        if (rowCount === 0) break;
        await new Promise((r) => setTimeout(r, 20));
      }
      await expect(sink.count()).rejects.toThrow();

      // The server released the lock with the session.
      const other = new PostgresSink(databaseUrl!, { table });
      try {
        await other.lock(FILE);
      } finally {
        await other.close();
      }
    } finally {
      await admin.end();
      await sink.close().catch(() => {});
    }
  });

  describe('retrying a rolled-back batch', () => {
    const retryTable = `${table}_retry`;
    const seq = `${retryTable}_attempts`;
    const fn = `${retryTable}_fail`;
    let admin: pg.Client;
    let sink: PostgresSink;

    beforeEach(async () => {
      sink = new PostgresSink(databaseUrl!, { table: retryTable });
      await sink.open();
      admin = new pg.Client({ connectionString: databaseUrl });
      await admin.connect();
      await admin.query(`CREATE SEQUENCE ${seq}`);
    });

    afterEach(async () => {
      await sink.close();
      await admin.query(`DROP TABLE IF EXISTS ${retryTable}`);
      await admin.query(`DROP FUNCTION IF EXISTS ${fn}()`);
      await admin.query(`DROP SEQUENCE IF EXISTS ${seq}`);
      await admin.end();
      await dropTable(retryTable);
    });

    /**
     * Makes the first `failures` inserts raise `code`. Sequences are not rolled back, so the
     * sequence counts attempts across ROLLBACKs. nextval runs per row: tests use single-row
     * batches so one row is one attempt.
     */
    const failFirst = async (failures: number, code: string): Promise<void> => {
      await admin.query(`
        CREATE FUNCTION ${fn}() RETURNS trigger LANGUAGE plpgsql AS $$
        BEGIN
          IF nextval('${seq}') <= ${failures} THEN
            RAISE EXCEPTION 'forced' USING ERRCODE = '${code}';
          END IF;
          RETURN NEW;
        END $$`);
      await admin.query(
        `CREATE TRIGGER force_failure BEFORE INSERT ON ${retryTable}
         FOR EACH ROW EXECUTE FUNCTION ${fn}()`,
      );
    };

    const attempts = async (): Promise<number> => {
      const { rows } = await admin.query<{ n: string }>(
        `SELECT CASE WHEN is_called THEN last_value ELSE 0 END AS n FROM ${seq}`,
      );
      return Number(rows[0]!.n);
    };

    it('retries a serialization failure and then commits batch and checkpoint', async () => {
      await failFirst(2, '40001');
      await sink.writeBatch([tx(1)], checkpointAt(1));
      expect(await attempts()).toBe(3);
      expect(await sink.count()).toBe(1);
      expect(await sink.checkpoints.load(FILE)).toEqual(checkpointAt(1));
      expect(sink.stats).toEqual({ directBatches: 1, mergedBatches: 0 });
    });

    it('gives up after three attempts and leaves nothing behind', async () => {
      await failFirst(100, '40001');
      await expect(sink.writeBatch([tx(1)], checkpointAt(1))).rejects.toMatchObject({
        code: '40001',
      });
      expect(await attempts()).toBe(3);
      expect(await sink.count()).toBe(0);
      expect(await sink.checkpoints.load(FILE)).toBeUndefined();
    });

    it('does not retry other errors', async () => {
      await failFirst(100, '22023');
      await expect(sink.writeBatch([tx(1)])).rejects.toMatchObject({ code: '22023' });
      expect(await attempts()).toBe(1);
    });
  });

  it('rejects table names that are not plain identifiers', () => {
    expect(() => new PostgresSink(databaseUrl!, { table: 'x; DROP TABLE y' })).toThrow();
  });
});
