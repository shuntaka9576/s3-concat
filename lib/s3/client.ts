import { Readable } from 'node:stream';
import {
  CompleteMultipartUploadCommand,
  CreateMultipartUploadCommand,
  GetObjectCommand,
  ListObjectsV2Command,
  type ListObjectsV2CommandOutput,
  UploadPartCommand,
  UploadPartCopyCommand,
} from '@aws-sdk/client-s3';
import pLimit from 'p-limit';
import type { S3Client } from '../s3-concat';
import type { S3File } from './file';
import { type UploadTask, getPartSizeForPartTask } from './task';

type S3FileInfo = { key: string; size: number; lastModified: Date };

const hasValidS3Properties = (content: {
  Key?: string;
  Size?: number;
  LastModified?: Date;
}): content is { Key: string; Size: number; LastModified: Date } =>
  content.Key !== undefined &&
  content.Size !== undefined &&
  content.LastModified !== undefined;

const isNotDirectory = (content: {
  Key: string;
}): boolean => !content.Key.endsWith('/');

export const getListFiles = async (
  s3Client: S3Client,
  bucketName: string,
  prefix: string
): Promise<S3FileInfo[]> => {
  const fileList: S3FileInfo[] = [];
  let continuationToken: string | undefined;

  do {
    const response: ListObjectsV2CommandOutput = await s3Client.send(
      new ListObjectsV2Command({
        Bucket: bucketName,
        Prefix: prefix,
        ContinuationToken: continuationToken,
      })
    );

    for (const content of response.Contents ?? []) {
      if (!hasValidS3Properties(content) || !isNotDirectory(content)) {
        continue;
      }
      fileList.push({
        key: content.Key,
        size: content.Size,
        lastModified: content.LastModified,
      });
    }

    continuationToken = response.IsTruncated
      ? response.NextContinuationToken
      : undefined;
  } while (continuationToken !== undefined);

  return fileList;
};

const encodeCopySourceKey = (key: string): string =>
  key.split('/').map(encodeURIComponent).join('/');

// S3's SigV4 chunked transfer encoding requires every non-final chunk to be
// at least 8192 bytes. GetObject bodies of small source files can yield
// chunks well below that, which makes UploadPartCommand fail with
// InvalidChunkSizeError against real S3 (LocalStack/floci don't enforce it).
// Coalesce yielded buffers up to MIN_STREAM_CHUNK before passing them on;
// the very last chunk is allowed to be smaller and is flushed at the end.
const MIN_STREAM_CHUNK = 64 * 1024;

const createCombinedStream = (
  s3Client: S3Client,
  s3Files: S3File[],
  bucketName: string
): Readable =>
  Readable.from(
    (async function* () {
      const pending: Buffer[] = [];
      let pendingSize = 0;

      for (const f of s3Files) {
        const range = `bytes=${f.start}-${f.size - 1}`;
        const getRes = await s3Client.send(
          new GetObjectCommand({
            Bucket: bucketName,
            Key: f.key,
            Range: range,
          })
        );

        const body = getRes.Body;
        if (body === undefined) {
          throw new Error(`empty body for s3://${bucketName}/${f.key}`);
        }
        for await (const chunk of body as Readable) {
          const buf = chunk as Buffer;
          pending.push(buf);
          pendingSize += buf.length;
          if (pendingSize >= MIN_STREAM_CHUNK) {
            yield Buffer.concat(pending, pendingSize);
            pending.length = 0;
            pendingSize = 0;
          }
        }
      }
      if (pendingSize > 0) {
        yield Buffer.concat(pending, pendingSize);
      }
    })()
  );

export const concatWithMultipartUpload = async (
  s3Client: S3Client,
  bucketName: string,
  newKey: string,
  tasks: UploadTask[],
  limitNumber: number
): Promise<void> => {
  const createRes = await s3Client.send(
    new CreateMultipartUploadCommand({
      Bucket: bucketName,
      Key: newKey,
    })
  );
  const uploadId = createRes.UploadId;
  if (uploadId === undefined) {
    throw new Error('request failed CreateMultipartUploadCommand');
  }

  const completedParts: Array<
    { ETag?: string; PartNumber: number } | undefined
  > = new Array(tasks.length);

  const limit = pLimit(limitNumber);

  await Promise.all(
    tasks.map((task, i) =>
      limit(async () => {
        const partNumber = i + 1;

        if (task.uploadType === 'PartCopy') {
          const copySource = `${bucketName}/${encodeCopySourceKey(task.s3File.key)}`;
          const copyRange = `bytes=${task.start}-${task.end - 1}`;
          const copyRes = await s3Client.send(
            new UploadPartCopyCommand({
              Bucket: bucketName,
              Key: newKey,
              UploadId: uploadId,
              PartNumber: partNumber,
              CopySource: copySource,
              CopySourceRange: copyRange,
            })
          );

          completedParts[i] = {
            ETag: copyRes.CopyPartResult?.ETag,
            PartNumber: partNumber,
          };
          return;
        }

        const partSize = getPartSizeForPartTask(task);
        if (partSize === 0) {
          return;
        }

        const partStream = createCombinedStream(
          s3Client,
          task.s3Files,
          bucketName
        );

        const uploadRes = await s3Client.send(
          new UploadPartCommand({
            Bucket: bucketName,
            Key: newKey,
            UploadId: uploadId,
            PartNumber: partNumber,
            Body: partStream,
            ContentLength: partSize,
          })
        );

        completedParts[i] = {
          ETag: uploadRes.ETag,
          PartNumber: partNumber,
        };
      })
    )
  );

  await s3Client.send(
    new CompleteMultipartUploadCommand({
      Bucket: bucketName,
      Key: newKey,
      UploadId: uploadId,
      MultipartUpload: {
        Parts: completedParts.filter(
          (p): p is { ETag?: string; PartNumber: number } => p !== undefined
        ),
      },
    })
  );
};
