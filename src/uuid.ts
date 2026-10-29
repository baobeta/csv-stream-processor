import { randomBytes } from 'node:crypto';

/**
 * UUIDv7 (RFC 9562): 48-bit Unix milliseconds, then random bits. Keys generated later sort
 * later, so B-tree inserts append to the rightmost page instead of touching random pages the
 * way UUIDv4 does. Node 24 has no built-in `crypto.randomUUIDv7()` yet.
 *
 * Ordering is only guaranteed across milliseconds; ids within the same millisecond are random
 * relative to each other, which is fine for index locality.
 */
export function uuidv7(now: number = Date.now()): string {
  const bytes = randomBytes(16);
  bytes.writeUIntBE(now, 0, 6);
  bytes[6] = (bytes[6]! & 0x0f) | 0x70; // version 7
  bytes[8] = (bytes[8]! & 0x3f) | 0x80; // RFC 9562 variant
  const hex = bytes.toString('hex');
  return `${hex.slice(0, 8)}-${hex.slice(8, 12)}-${hex.slice(12, 16)}-${hex.slice(16, 20)}-${hex.slice(20)}`;
}
