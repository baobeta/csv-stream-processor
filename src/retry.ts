import { setTimeout as sleep } from 'node:timers/promises';

export interface RetryOptions {
  /** Total tries, including the first one. */
  attempts: number;
  baseDelayMs: number;
  isRetryable: (error: unknown) => boolean;
}

/** Runs `work`, retrying with jittered exponential backoff while `isRetryable` accepts the error. */
export async function withRetry<T>(work: () => Promise<T>, options: RetryOptions): Promise<T> {
  for (let attempt = 1; ; attempt++) {
    try {
      return await work();
    } catch (error) {
      if (attempt >= options.attempts || !options.isRetryable(error)) throw error;
      // Exponential backoff with full jitter, so two conflicting importers don't retry in lockstep.
      await sleep(Math.random() * options.baseDelayMs * 2 ** (attempt - 1));
    }
  }
}
