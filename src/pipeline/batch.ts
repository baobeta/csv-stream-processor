/**
 * Groups items from an async iterable into arrays of `size`.
 *
 * Writing rows one by one means one DB round-trip (or transaction) per row; batching amortises
 * that cost. Because this is a pull-based generator, the upstream parser only reads more of the
 * file when the sink asks for the next batch - that is backpressure for free.
 */
export async function* batch<T>(source: AsyncIterable<T>, size: number): AsyncGenerator<T[]> {
  if (!Number.isInteger(size) || size < 1) {
    throw new RangeError(`batch size must be a positive integer, got ${size}`);
  }

  let buffer: T[] = [];
  for await (const item of source) {
    buffer.push(item);
    if (buffer.length >= size) {
      yield buffer;
      buffer = [];
    }
  }
  if (buffer.length > 0) yield buffer;
}
