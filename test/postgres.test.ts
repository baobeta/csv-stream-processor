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

  it('rejects table names that are not plain identifiers', () => {
    expect(() => new PostgresSink(databaseUrl!, { table: 'x; DROP TABLE y' })).toThrow();
  });
});
