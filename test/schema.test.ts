import { describe, expect, it } from 'vitest';
import { transactionSchema } from '../src/schema.ts';

const valid = {
  id: '3b241101-e2bb-4255-8caf-4136c566a962',
  user_email: 'a@example.com',
  amount: '12.5',
  currency: 'USD',
  status: 'completed',
  created_at: '2026-01-02T03:04:05.000Z',
  description: 'Coffee',
};

describe('transactionSchema', () => {
  it('converts amount to integer minor units without float drift', () => {
    expect(transactionSchema.parse(valid).amount).toBe(1250);
    expect(transactionSchema.parse({ ...valid, amount: '0.29' }).amount).toBe(29);
  });

  it.each([
    ['user_email', 'nope'],
    ['amount', '-1.00'],
    ['amount', '1.234'],
    ['currency', 'XXX'],
    ['created_at', '2024-13-45'],
  ])('rejects invalid %s = %s', (field, value) => {
    expect(transactionSchema.safeParse({ ...valid, [field]: value }).success).toBe(false);
  });
});
