import type { Readable } from 'node:stream';
import { describe, expect, test } from 'vitest';
import {
  COALESCE_MIN,
  CoalesceTransform,
} from '../../../lib/s3/coalesce-transform';

const drain = (stream: Readable): Promise<Buffer[]> =>
  new Promise<Buffer[]>((resolve, reject) => {
    const chunks: Buffer[] = [];
    stream.on('data', (c: Buffer) => chunks.push(c));
    stream.on('end', () => resolve(chunks));
    stream.on('error', reject);
  });

const writeAll = (
  coalesce: CoalesceTransform,
  inputs: Buffer[]
): Promise<void> =>
  new Promise<void>((resolve, reject) => {
    let i = 0;
    const next = (): void => {
      while (i < inputs.length) {
        const ok = coalesce.write(inputs[i] as Buffer);
        i += 1;
        if (!ok) {
          coalesce.once('drain', next);
          return;
        }
      }
      coalesce.end();
      resolve();
    };
    coalesce.on('error', reject);
    next();
  });

describe('CoalesceTransform', () => {
  test('coalesces sub-threshold chunks until threshold is reached', async () => {
    const coalesce = new CoalesceTransform();
    const drained = drain(coalesce);

    // 4 KiB × 16 = 64 KiB exactly hits the threshold.
    const small = Buffer.alloc(4 * 1024, 0x41);
    await writeAll(
      coalesce,
      Array.from({ length: 16 }, () => small)
    );

    const chunks = await drained;
    expect(chunks.length).toBe(1);
    expect((chunks[0] as Buffer).byteLength).toBe(COALESCE_MIN);
  });

  test('passes a single large chunk through immediately', async () => {
    const coalesce = new CoalesceTransform();
    const drained = drain(coalesce);

    const big = Buffer.alloc(COALESCE_MIN * 2 + 17, 0x42);
    await writeAll(coalesce, [big]);

    const chunks = await drained;
    expect(chunks.length).toBe(1);
    expect((chunks[0] as Buffer).byteLength).toBe(big.byteLength);
  });

  test('emits remaining bytes from _flush even if smaller than threshold', async () => {
    const coalesce = new CoalesceTransform();
    const drained = drain(coalesce);

    const tail = Buffer.from('trailing-bytes');
    await writeAll(coalesce, [tail]);

    const chunks = await drained;
    expect(chunks.length).toBe(1);
    expect((chunks[0] as Buffer).toString()).toBe('trailing-bytes');
  });

  test('chains multiple coalesced chunks while crossing threshold repeatedly', async () => {
    const coalesce = new CoalesceTransform();
    const drained = drain(coalesce);

    const block = Buffer.alloc(40 * 1024, 0x43); // 40 KiB
    // 40 KiB ×5 = 200 KiB → coalesces fire at the >=64 KiB boundary on
    // every other write (40 → 80 → 120 → 160 → 200). With current _transform
    // logic that yields three pushes plus a final flushed remainder.
    await writeAll(
      coalesce,
      Array.from({ length: 5 }, () => block)
    );

    const chunks = await drained;
    const totalBytes = chunks.reduce((sum, c) => sum + c.byteLength, 0);
    expect(totalBytes).toBe(block.byteLength * 5);
    for (const c of chunks.slice(0, -1)) {
      expect(c.byteLength).toBeGreaterThanOrEqual(COALESCE_MIN);
    }
  });

  test('propagates upstream errors via destroy', async () => {
    const coalesce = new CoalesceTransform();
    const err = new Error('boom');

    const failed: Promise<Error> = new Promise((resolve) => {
      coalesce.on('error', (e: Error) => resolve(e));
    });

    coalesce.destroy(err);

    expect(await failed).toBe(err);
  });
});
