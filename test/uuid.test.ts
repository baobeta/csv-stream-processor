import { describe, expect, it } from 'vitest';
import { z } from 'zod';
import { uuidv7 } from '../src/uuid.ts';

describe('uuidv7', () => {
  it('produces RFC 9562 version 7 UUIDs', () => {
    const id = uuidv7();
    expect(id).toMatch(/^[0-9a-f]{8}-[0-9a-f]{4}-7[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/);
    expect(z.uuid().safeParse(id).success).toBe(true);
  });

  it('encodes the timestamp so later ids sort later', () => {
    const ids = [1_700_000_000_000, 1_700_000_000_001, 1_800_000_000_000].map((t) => uuidv7(t));
    expect([...ids].sort()).toEqual(ids);
    expect(ids[0]!.replaceAll('-', '').slice(0, 12)).toBe(
      (1_700_000_000_000).toString(16).padStart(12, '0'),
    );
  });
});
