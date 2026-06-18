import { Readable } from 'node:stream';
import { describe, expect, test, vi } from 'vitest';
import { S3File } from '../../../lib/s3/file';
import { buildMergedBody } from '../../../lib/s3/stream-body';

type SendCall = (command: unknown) => Promise<unknown>;

const drain = (stream: Readable): Promise<Buffer> =>
  new Promise<Buffer>((resolve, reject) => {
    const chunks: Buffer[] = [];
    stream.on('data', (c: Buffer) => chunks.push(c));
    stream.on('end', () => resolve(Buffer.concat(chunks)));
    stream.on('error', reject);
  });

const makeBody = (payload: Buffer | Buffer[]): Readable => {
  const arr = Array.isArray(payload) ? payload : [payload];
  return Readable.from(arr);
};

describe('buildMergedBody', () => {
  test('concatenates multiple GetObject bodies in order', async () => {
    const files = [
      new S3File('a.txt', 5, 0),
      new S3File('b.txt', 4, 0),
      new S3File('c.txt', 3, 0),
    ];
    const payloads: Record<string, Buffer> = {
      'a.txt': Buffer.from('AAAAA'),
      'b.txt': Buffer.from('BBBB'),
      'c.txt': Buffer.from('CCC'),
    };

    const send = vi.fn<SendCall>(async (command) => {
      const key = (command as { input: { Key: string } }).input.Key;
      return { Body: makeBody(payloads[key] as Buffer) };
    });

    const controller = new AbortController();
    const merged = buildMergedBody({
      s3Client: { send },
      bucketName: 'b',
      s3Files: files,
      signal: controller.signal,
      contentLength: 12,
    });

    const result = await drain(merged.stream);
    expect(result.toString()).toBe(`${'AAAAA'}${'BBBB'}${'CCC'}`);
    expect(merged.contentLength).toBe(12);
    expect(send).toHaveBeenCalledTimes(3);
  });

  test('propagates GetObject errors as a stream error', async () => {
    const files = [new S3File('a.txt', 4, 0)];
    const send = vi.fn<SendCall>(async () => {
      throw new Error('get-object failure');
    });

    const merged = buildMergedBody({
      s3Client: { send },
      bucketName: 'b',
      s3Files: files,
      signal: new AbortController().signal,
      contentLength: 4,
    });

    await expect(drain(merged.stream)).rejects.toThrow('get-object failure');
  });

  test('exposes the upstream cause via firstError()', async () => {
    const files = [new S3File('a.txt', 4, 0)];
    const cause = new Error('SlowDown');
    const send = vi.fn<SendCall>(async () => {
      throw cause;
    });

    const merged = buildMergedBody({
      s3Client: { send },
      bucketName: 'b',
      s3Files: files,
      signal: new AbortController().signal,
      contentLength: 4,
    });

    await expect(drain(merged.stream)).rejects.toBeDefined();
    expect(merged.firstError()).toBe(cause);
  });

  test('throws "empty body" when GetObject returns no Body', async () => {
    const files = [new S3File('a.txt', 1, 0)];
    const send = vi.fn<SendCall>(async () => ({ Body: undefined }));

    const merged = buildMergedBody({
      s3Client: { send },
      bucketName: 'b',
      s3Files: files,
      signal: new AbortController().signal,
      contentLength: 1,
    });

    await expect(drain(merged.stream)).rejects.toThrow(/empty body/);
  });

  test('aborting during an in-flight GetObject prevents further requests', async () => {
    const files = [
      new S3File('a.txt', 3, 0),
      new S3File('b.txt', 3, 0),
      new S3File('c.txt', 3, 0),
    ];

    // The first send hangs forever so the loop never advances on its own,
    // letting us observe that abort stops it before file b/c are requested.
    const send = vi.fn<SendCall>(() => new Promise<never>(() => undefined));

    const controller = new AbortController();
    const merged = buildMergedBody({
      s3Client: { send },
      bucketName: 'b',
      s3Files: files,
      signal: controller.signal,
      contentLength: 9,
    });

    const drained = drain(merged.stream);

    await vi.waitFor(() => {
      expect(send).toHaveBeenCalledTimes(1);
    });

    controller.abort(new Error('cancelled'));

    await expect(drained).rejects.toThrow('cancelled');
    await new Promise<void>((resolve) => setImmediate(resolve));
    expect(send.mock.calls.length).toBe(1);
  });
});
