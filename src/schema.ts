import { z } from 'zod';

export const CURRENCIES = ['USD', 'EUR', 'GBP', 'SGD'] as const;
export const STATUSES = ['pending', 'completed', 'failed'] as const;

export const CSV_COLUMNS = [
  'id',
  'user_email',
  'amount',
  'currency',
  'status',
  'created_at',
  'description',
] as const;

/**
 * Money is stored as integer minor units (cents). Parsing the decimal string by hand avoids
 * float drift: Number('0.29') * 100 === 28.999999999999996.
 */
function toMinorUnits(value: string): number {
  const [whole = '0', fraction = ''] = value.split('.');
  return Number(whole) * 100 + Number(fraction.padEnd(2, '0'));
}

/**
 * Every CSV cell arrives as a string. The schema is the single place where raw text becomes
 * typed data, so the rest of the pipeline can trust its input.
 */
export const transactionSchema = z.object({
  id: z.uuid(),
  user_email: z.email(),
  amount: z
    .string()
    .regex(/^\d{1,12}(\.\d{1,2})?$/, 'must be a positive decimal with at most 2 fraction digits')
    .transform(toMinorUnits),
  currency: z.enum(CURRENCIES),
  status: z.enum(STATUSES),
  created_at: z.iso.datetime(),
  description: z.string().max(500),
});

export type Transaction = z.output<typeof transactionSchema>;
