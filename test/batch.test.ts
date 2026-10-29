import { describe, expect, it } from 'vitest';
import { batch } from '../src/pipeline/batch.ts';

async function* range(n: number) {
  for (let i = 0; i < n; i++) yield i;
}

async function collect<T>(source: AsyncIterable<T>): Promise<T[]> {
  const out: T[] = [];
  for await (const item of source) out.push(item);
  return out;
}

describe('batch', () => {
  it('groups items and flushes the remainder', async () => {
    expect(await collect(batch(range(7), 3))).toEqual([[0, 1, 2], [3, 4, 5], [6]]);
  });

  it('yields nothing for an empty source', async () => {
    expect(await collect(batch(range(0), 3))).toEqual([]);
  });

  it('rejects a non-positive size', async () => {
    await expect(collect(batch(range(1), 0))).rejects.toThrow(RangeError);
  });
});
