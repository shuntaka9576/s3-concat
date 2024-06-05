import type { Readable } from 'node:stream';
import {
  type GetObjectCommandOutput,
  ListObjectsV2Command,
  type ListObjectsV2CommandOutput,
} from '@aws-sdk/client-s3';
import { GetObjectCommand } from '@aws-sdk/client-s3';
import type { S3Client } from './s3-concat';

export const getListFiles = async (
  s3Client: S3Client,
  bucketName: string,
  prefix: string
): Promise<{ key: string; size: number }[]> => {
  let isTruncated = true;
  let continuationToken: string | undefined = undefined;
  const fileList: { key: string; size: number }[] = [];

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
        (
          content:
            | { Key: string; Size: number }
            | { Key: string; Size: undefined }
            | { Key: undefined; Size: number }
            | { Key: undefined; Size: undefined }
        ) => content.Key !== undefined && content.Size !== undefined
      )
        .filter((content) => content.Key.endsWith('/') === false)
        .map((content) => ({ key: content.Key, size: content.Size }));

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
