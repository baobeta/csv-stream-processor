import { describe, expect, it } from 'vitest';
import { checkHeader, HeaderMismatchError } from '../src/pipeline/header.ts';

const GOOD = ['id', 'user_email', 'amount', 'currency', 'status', 'created_at', 'description'];

describe('checkHeader', () => {
  it('accepts the expected columns in any order', () => {
    const shuffled = [...GOOD].reverse();
    expect(checkHeader(shuffled)).toEqual(shuffled);
  });

  it('lists missing and unexpected columns', () => {
    const header = GOOD.map((c) => (c === 'amount' ? 'amt' : c));
    expect(() => checkHeader(header)).toThrow(
      'CSV header does not match the expected columns (missing: amount; unexpected: amt)',
    );
  });

  it('treats a repeated column as unexpected', () => {
    try {
      checkHeader([...GOOD, 'id']);
      expect.unreachable();
    } catch (error) {
      expect(error).toBeInstanceOf(HeaderMismatchError);
      expect(error).toMatchObject({ missing: [], unexpected: ['id (duplicate)'] });
    }
  });
});
