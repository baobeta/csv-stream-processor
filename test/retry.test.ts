import { describe, expect, it } from 'vitest';
import { withRetry } from '../src/retry.ts';

const retryable = Object.assign(new Error('deadlock'), { code: '40P01' });
const fatal = Object.assign(new Error('bad data'), { code: '22001' });
const options = {
  attempts: 3,
  baseDelayMs: 1,
  isRetryable: (e: unknown) => (e as { code?: string }).code === '40P01',
};

/** Fails with the given errors in order, then succeeds. */
function flaky(errors: Error[]) {
  let calls = 0;
  const work = () => {
    calls++;
    const error = errors.shift();
    return error ? Promise.reject(error) : Promise.resolve('ok');
  };
  return { work, calls: () => calls };
}

describe('withRetry', () => {
  it('retries retryable errors until the work succeeds', async () => {
    const f = flaky([retryable, retryable]);
    await expect(withRetry(f.work, options)).resolves.toBe('ok');
    expect(f.calls()).toBe(3);
  });

  it('gives up after the last attempt with the last error', async () => {
    const f = flaky([retryable, retryable, retryable]);
    await expect(withRetry(f.work, options)).rejects.toBe(retryable);
    expect(f.calls()).toBe(3);
  });

  it('does not retry other errors', async () => {
    const f = flaky([fatal]);
    await expect(withRetry(f.work, options)).rejects.toBe(fatal);
    expect(f.calls()).toBe(1);
  });
});
