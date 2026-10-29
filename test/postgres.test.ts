import pg from 'pg';
import { afterAll, describe, expect, it } from 'vitest';
import { PostgresSink } from '../src/sinks/postgres.ts';
import { tx } from './helpers.ts';

const databaseUrl = process.env.DATABASE_URL;
const table = `test_transactions_${process.pid}`;

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

  it('rejects table names that are not plain identifiers', () => {
    expect(() => new PostgresSink(databaseUrl!, { table: 'x; DROP TABLE y' })).toThrow();
  });
});
