import type { Readable } from 'node:stream';
import { GetObjectCommand } from '@aws-sdk/client-s3';
import type { S3Client } from '../s3-concat';
import { CoalesceTransform } from './coalesce-transform';
import type { S3File } from './file';

export interface BuildMergedBodyOptions {
  s3Client: S3Client;
  bucketName: string;
  s3Files: S3File[];
  signal: AbortSignal;
  contentLength: number;
}

export interface MergedBody {
  stream: Readable;
  contentLength: number;
  /**
   * Returns the first error captured while pulling source bytes (e.g. a
   * `GetObject` SlowDown or socket timeout). Use it to recover the real
   * cause when `UploadPart` rejects with the SDK's generic AbortError —
   * destroying the body stream makes the SDK surface "aborted" instead
   * of the upstream failure.
   */
  firstError: () => Error | undefined;
}

const writeChunkWithBackpressure = (
  coalesce: CoalesceTransform,
  body: Readable
): Promise<void> =>
  new Promise<void>((resolve, reject) => {
    const onData = (chunk: Buffer): void => {
      if (!coalesce.write(chunk)) {
        body.pause();
        coalesce.once('drain', () => {
          body.resume();
        });
      }
    };
    const onEnd = (): void => {
      cleanup();
      resolve();
    };
    const onError = (err: Error): void => {
      cleanup();
      reject(err);
    };
    const cleanup = (): void => {
      body.off('data', onData);
      body.off('end', onEnd);
      body.off('error', onError);
    };
    body.on('data', onData);
    body.on('end', onEnd);
    body.on('error', onError);
  });

export const buildMergedBody = (opts: BuildMergedBodyOptions): MergedBody => {
  const coalesce = new CoalesceTransform();
  let activeBody: Readable | undefined;
  let firstError: Error | undefined;

  const capture = (err: Error): void => {
    if (firstError === undefined) firstError = err;
  };

  const abortListener = (): void => {
    const reason = (opts.signal as AbortSignal & { reason?: unknown }).reason;
    const err = reason instanceof Error ? reason : new Error('aborted');
    capture(err);
    activeBody?.destroy(err);
    coalesce.destroy(err);
  };
  opts.signal.addEventListener('abort', abortListener, { once: true });

  const run = async (): Promise<void> => {
    for (const f of opts.s3Files) {
      if (opts.signal.aborted) {
        throw new Error('aborted');
      }
      const range = `bytes=${f.start}-${f.size - 1}`;
      const getRes = await opts.s3Client.send(
        new GetObjectCommand({
          Bucket: opts.bucketName,
          Key: f.key,
          Range: range,
        })
      );
      const body = getRes.Body as Readable | undefined;
      if (body === undefined) {
        throw new Error(`empty body for s3://${opts.bucketName}/${f.key}`);
      }
      activeBody = body;
      try {
        await writeChunkWithBackpressure(coalesce, body);
      } finally {
        activeBody = undefined;
      }
    }
    coalesce.end();
  };

  run()
    .catch((err: unknown) => {
      const error = err instanceof Error ? err : new Error(String(err));
      capture(error);
      coalesce.destroy(error);
    })
    .finally(() => {
      opts.signal.removeEventListener('abort', abortListener);
    });

  return {
    stream: coalesce,
    contentLength: opts.contentLength,
    firstError: () => firstError,
  };
};
