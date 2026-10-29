import type { Transaction } from '../src/schema.ts';

export const HEADER = 'id,user_email,amount,currency,status,created_at,description';

export const uuid = (n: number) => `00000000-0000-4000-8000-${String(n).padStart(12, '0')}`;

export const csvRow = (id: string, overrides: { amount?: string; description?: string } = {}) =>
  `${id},a@example.com,${overrides.amount ?? '1.00'},USD,completed,2026-01-02T03:04:05.000Z,${overrides.description ?? 'Coffee'}`;

export const tx = (n: number, overrides: Partial<Transaction> = {}): Transaction => ({
  id: uuid(n),
  user_email: 'a@example.com',
  amount: 100,
  currency: 'USD',
  status: 'completed',
  created_at: '2026-01-02T03:04:05.000Z',
  description: 'Coffee',
  ...overrides,
});
