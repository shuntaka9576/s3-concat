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

// Collect all source-file bytes for one PartTask into a single Buffer.
//
// UploadPartCommand selects its wire encoding based on the Body type:
// a Buffer payload is sent with a known Content-Length (single-chunk
// SigV4), while a Readable triggers aws-chunked streaming, which S3
// gates on a 8192-byte minimum per non-final chunk. Streaming would
// make us depend on the consumer's @aws-sdk/client-s3 version (3.97x+
// stopped buffering input streams internally, so small GetObject body
// chunks reach the wire intact and S3 rejects them). Collecting into
// a Buffer sidesteps the chunked path entirely.
//
// Memory cost is bounded by S3 itself: planedUploadTask caps each
// PartTask at PART_UPLOAD_LIMIT (5 MiB), so even with a saturated
// pLimit the in-flight peak is roughly partSize * concurrency.
const collectPartBytes = async (
  s3Client: S3Client,
  s3Files: S3File[],
  bucketName: string
): Promise<Buffer> => {
  const parts: Buffer[] = [];
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
    const bytes = await body.transformToByteArray();
    parts.push(Buffer.from(bytes.buffer, bytes.byteOffset, bytes.byteLength));
  }
  return Buffer.concat(parts);
};

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

        const partBytes = await collectPartBytes(
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
            Body: partBytes,
            ContentLength: partBytes.length,
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
