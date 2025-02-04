import { PassThrough, type Readable } from 'node:stream';
import { pipeline } from 'node:stream/promises';
import type {
  GetObjectCommandOutput,
  ListObjectsV2CommandOutput,
} from '@aws-sdk/client-s3';

import {
  CompleteMultipartUploadCommand,
  CreateMultipartUploadCommand,
  GetObjectCommand,
  ListObjectsV2Command,
  UploadPartCommand,
  UploadPartCopyCommand,
} from '@aws-sdk/client-s3';
import type { S3Client } from '../s3-concat';
import type { S3File } from './file';
import { type UploadTask, getPartSizeForPartTask } from './task';

function hasValidS3Properties(content: {
  Key?: string;
  Size?: number;
  LastModified?: Date;
}): content is { Key: string; Size: number; LastModified: Date } {
  return (
    content.Key !== undefined &&
    content.Size !== undefined &&
    content.LastModified !== undefined
  );
}

function isNotDirectory(content: {
  Key: string;
  Size: number;
  LastModified: Date;
}): content is { Key: string; Size: number; LastModified: Date } {
  return !content.Key.endsWith('/');
}

export const getListFiles = async (
  s3Client: S3Client,
  bucketName: string,
  prefix: string
): Promise<{ key: string; size: number; lastModified: Date }[]> => {
  let isTruncated = true;
  let continuationToken: string | undefined = undefined;
  const fileList: { key: string; size: number; lastModified: Date }[] = [];

  while (isTruncated) {
    const response: ListObjectsV2CommandOutput = await s3Client.send(
      new ListObjectsV2Command({
        Bucket: bucketName,
        Prefix: prefix,
        ContinuationToken: continuationToken,
      })
    );

    if (response.Contents) {
      const files = response.Contents.filter(
        (content) =>
          content.Key !== undefined &&
          content.Size !== undefined &&
          content.LastModified !== undefined
      )
        .filter(hasValidS3Properties)
        .filter(isNotDirectory)
        .map((content) => ({
          key: content.Key,
          size: content.Size,
          lastModified: content.LastModified,
        }));

      fileList.push(...files);
    }

    isTruncated = response.IsTruncated ?? false;
    continuationToken = response.NextContinuationToken;
  }

  return fileList;
};

export const getStream = async (
  s3Client: S3Client,
  bucket: string,
  key: string
): Promise<Readable> => {
  const data: GetObjectCommandOutput = await s3Client.send(
    new GetObjectCommand({
      Bucket: bucket,
      Key: key,
    })
  );

  const body = data.Body as Readable;

  return body;
};

export const concatWithMultipartUpload = async (
  s3Client: S3Client,
  bucketName: string,
  newKey: string,
  tasks: UploadTask[]
) => {
  const createRes = await s3Client.send(
    new CreateMultipartUploadCommand({
      Bucket: bucketName,
      Key: newKey,
    })
  );
  const uploadId = createRes.UploadId;
  if (uploadId == null) {
    throw new Error('request failed CreateMultipartUploadCommand');
  }

  const completedParts: Array<{ ETag?: string; PartNumber: number }> = [];
  let partNumber = 1;

  for (const task of tasks) {
    if (task.uploadType === 'PartCopy') {
      const copySource = encodeURI(`${bucketName}/${task.s3File.key}`);
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
      completedParts.push({
        ETag: copyRes.CopyPartResult?.ETag,
        PartNumber: partNumber,
      });
    } else {
      const partSize = getPartSizeForPartTask(task);
      if (partSize === 0) {
        continue;
      }

      const partStream = await createCombinedStream(
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

      completedParts.push({
        ETag: uploadRes.ETag,
        PartNumber: partNumber,
      });
    }
    partNumber++;
  }

  await s3Client.send(
    new CompleteMultipartUploadCommand({
      Bucket: bucketName,
      Key: newKey,
      UploadId: uploadId,
      MultipartUpload: {
        Parts: completedParts,
      },
    })
  );
};

const createCombinedStream = async (
  s3Client: S3Client,
  s3Files: S3File[],
  bucketName: string
): Promise<PassThrough> => {
  const pass = new PassThrough();

  process.nextTick(async () => {
    try {
      for (const f of s3Files) {
        const range = `bytes=${f.start}-${f.size - 1}`;

        const getRes = await s3Client.send(
          new GetObjectCommand({
            Bucket: bucketName,
            Key: f.key,
            Range: range,
          })
        );

        const bodyStream = getRes.Body as Readable;
        await pipeline(bodyStream, pass, { end: false });
      }
      pass.end();
    } catch (err) {
      pass.destroy(err as Error);
    }
  });

  return pass;
};
