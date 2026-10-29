import { mkdtemp, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { afterEach, beforeEach, describe, expect, it } from 'vitest';
import { SqliteSink } from '../src/sinks/sqlite.ts';
import { tx } from './helpers.ts';

let dir: string;
beforeEach(async () => {
  dir = await mkdtemp(join(tmpdir(), 'sqlite-sink-'));
});
afterEach(async () => {
  await rm(dir, { recursive: true, force: true });
});

describe('SqliteSink', () => {
  it('direct: inserts immediately and ignores duplicate ids', () => {
    const sink = new SqliteSink(join(dir, 'db.sqlite'));
    sink.open({ resuming: false });
    sink.writeBatch([tx(1), tx(2)]);
    sink.writeBatch([tx(2), tx(3)]);
    expect(sink.count()).toBe(3);
    sink.close();
  });

  it('staging: rows only reach the table on finish, deduplicated', () => {
    const sink = new SqliteSink(join(dir, 'db.sqlite'), { strategy: 'staging' });
    sink.open({ resuming: false });
    sink.writeBatch([tx(3), tx(1)]);
    sink.writeBatch([tx(1), tx(2)]);
    expect(sink.count()).toBe(0);

    sink.finish();
    expect(sink.count()).toBe(3);
    sink.close();
  });

  it('staging: a fresh run discards leftovers, a resumed run keeps them', () => {
    const path = join(dir, 'db.sqlite');
    const crashed = new SqliteSink(path, { strategy: 'staging' });
    crashed.open({ resuming: false });
    crashed.writeBatch([tx(1)]);
    crashed.close();

    const resumed = new SqliteSink(path, { strategy: 'staging' });
    resumed.open({ resuming: true });
    resumed.writeBatch([tx(2)]);
    resumed.finish();
    expect(resumed.count()).toBe(2);
    resumed.close();

    const leftover = new SqliteSink(path, { strategy: 'staging' });
    leftover.writeBatch([tx(9)]);
    leftover.close();
    const fresh = new SqliteSink(path, { strategy: 'staging' });
    fresh.open({ resuming: false });
    fresh.finish();
    expect(fresh.count()).toBe(2);
    fresh.close();
  });
});
