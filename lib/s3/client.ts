import {
  AbortMultipartUploadCommand,
  type ChecksumAlgorithm,
  type CompletedPart,
  CompleteMultipartUploadCommand,
  CreateMultipartUploadCommand,
  ListObjectsV2Command,
  type ListObjectsV2CommandOutput,
  UploadPartCommand,
  UploadPartCopyCommand,
} from '@aws-sdk/client-s3';
import type { S3Client } from '../s3-concat';
import type { LimitFunction } from '../std/concurrency';
import { buildMergedBody } from './stream-body';
import { getPartSizeForPartTask, type UploadTask } from './task';

type S3FileInfo = { key: string; size: number; lastModified: Date };

const hasValidS3Properties = (content: {
  Key?: string;
  Size?: number;
  LastModified?: Date;
}): content is { Key: string; Size: number; LastModified: Date } =>
  content.Key !== undefined &&
  content.Size !== undefined &&
  content.LastModified !== undefined;

const isNotDirectory = (content: { Key: string }): boolean =>
  !content.Key.endsWith('/');

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

// CRC32 keeps the streaming path on STREAMING-AWS4-HMAC-SHA256-PAYLOAD-TRAILER
// so the SDK never falls back to single-chunk SigV4, which would re-buffer the
// whole part in memory and re-introduce the hazard streaming is meant to avoid.
const STREAMING_CHECKSUM_ALGORITHM = 'CRC32' satisfies ChecksumAlgorithm;

export const concatWithMultipartUpload = async (
  s3Client: S3Client,
  srcBucketName: string,
  dstBucketName: string,
  newKey: string,
  tasks: UploadTask[],
  limit: LimitFunction
): Promise<void> => {
  const createRes = await s3Client.send(
    new CreateMultipartUploadCommand({
      Bucket: dstBucketName,
      Key: newKey,
      ChecksumAlgorithm: STREAMING_CHECKSUM_ALGORITHM,
    })
  );
  const uploadId = createRes.UploadId;
  if (uploadId === undefined) {
    throw new Error('request failed CreateMultipartUploadCommand');
  }

  const completedParts: Array<CompletedPart | undefined> = new Array(
    tasks.length
  );

  try {
    await Promise.all(
      tasks.map((task, i) =>
        limit(async () => {
          const partNumber = i + 1;

          if (task.uploadType === 'PartCopy') {
            const copySource = `${srcBucketName}/${encodeCopySourceKey(task.s3File.key)}`;
            const copyRange = `bytes=${task.start}-${task.end - 1}`;
            const copyRes = await s3Client.send(
              new UploadPartCopyCommand({
                Bucket: dstBucketName,
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
              ChecksumCRC32: copyRes.CopyPartResult?.ChecksumCRC32,
            };
            return;
          }

          const partSize = getPartSizeForPartTask(task);
          if (partSize === 0) {
            return;
          }

          const controller = new AbortController();
          const merged = buildMergedBody({
            s3Client,
            srcBucketName,
            s3Files: task.s3Files,
            signal: controller.signal,
            contentLength: partSize,
          });
          try {
            const uploadRes = await s3Client.send(
              new UploadPartCommand({
                Bucket: dstBucketName,
                Key: newKey,
                UploadId: uploadId,
                PartNumber: partNumber,
                Body: merged.stream,
                ContentLength: merged.contentLength,
                ChecksumAlgorithm: STREAMING_CHECKSUM_ALGORITHM,
              })
            );
            completedParts[i] = {
              ETag: uploadRes.ETag,
              PartNumber: partNumber,
              ChecksumCRC32: uploadRes.ChecksumCRC32,
            };
          } catch (err) {
            controller.abort();
            // The SDK surfaces a generic AbortError when we destroy the body
            // stream from buildMergedBody (e.g. on a GetObject failure). Pull
            // the original cause back out so SlowDown / 503 / socket timeout
            // / checksum mismatch survives instead of being masked.
            const upstream = merged.firstError();
            throw upstream ?? err;
          }
        })
      )
    );

    await s3Client.send(
      new CompleteMultipartUploadCommand({
        Bucket: dstBucketName,
        Key: newKey,
        UploadId: uploadId,
        MultipartUpload: {
          Parts: completedParts.filter(
            (p): p is CompletedPart => p !== undefined
          ),
        },
      })
    );
  } catch (err) {
    try {
      await s3Client.send(
        new AbortMultipartUploadCommand({
          Bucket: dstBucketName,
          Key: newKey,
          UploadId: uploadId,
        })
      );
    } catch {
      // Best-effort cleanup; surface the original error.
    }
    throw err;
  }
};
