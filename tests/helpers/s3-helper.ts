import { randomUUID } from 'node:crypto';
import {
  AbortMultipartUploadCommand,
  CreateBucketCommand,
  DeleteBucketCommand,
  DeleteObjectsCommand,
  ListMultipartUploadsCommand,
  ListObjectsV2Command,
  PutObjectCommand,
  type S3Client,
} from '@aws-sdk/client-s3';
import pLimit from '../../lib/std/concurrency';
import { onTestFinished } from 'vitest';

const limit = pLimit(10);

export class S3ClientHelper {
  private s3Client: S3Client;

  constructor(s3Client: S3Client) {
    this.s3Client = s3Client;
  }

  async setupS3(params: {
    files: {
      fileSize: number;
      fileCount: number;
      fill?: string;
    }[];
    prefix: string;
  }): Promise<{ bucketName: string }> {
    const bucketName = `bucket-${randomUUID()}`;

    await this.s3Client.send(
      new CreateBucketCommand({
        Bucket: bucketName,
      })
    );

    // Real AWS buckets cost money and Floci's container is recreated per run,
    // but the cleanup hook is cheap in both cases and keeps real-S3 mode safe.
    onTestFinished(() => this.cleanupBucket(bucketName));

    const promises = params.files.flatMap((file, i) => {
      const fileContent = Buffer.alloc(file.fileSize, file.fill ?? '0');

      const pl = [...Array(file.fileCount)].map((_, j) => {
        const fileName = `${params.prefix}/file-${i + 1}-${j + 1}.txt`;
        return limit(() => this.uploadFile(bucketName, fileName, fileContent));
      });

      return pl;
    });

    await Promise.all(promises);

    return {
      bucketName,
    };
  }

  uploadFile(bucketName: string, key: string, body: Buffer) {
    return this.s3Client.send(
      new PutObjectCommand({
        Bucket: bucketName,
        Key: key,
        Body: body,
      })
    );
  }

  private async cleanupBucket(bucketName: string): Promise<void> {
    // Abort lingering multipart uploads first so DeleteBucket isn't blocked.
    let uploadIdMarker: string | undefined;
    let keyMarker: string | undefined;
    do {
      const resp = await this.s3Client.send(
        new ListMultipartUploadsCommand({
          Bucket: bucketName,
          KeyMarker: keyMarker,
          UploadIdMarker: uploadIdMarker,
        })
      );
      for (const u of resp.Uploads ?? []) {
        if (u.Key === undefined || u.UploadId === undefined) continue;
        await this.s3Client.send(
          new AbortMultipartUploadCommand({
            Bucket: bucketName,
            Key: u.Key,
            UploadId: u.UploadId,
          })
        );
      }
      if (resp.IsTruncated) {
        keyMarker = resp.NextKeyMarker;
        uploadIdMarker = resp.NextUploadIdMarker;
      } else {
        keyMarker = undefined;
        uploadIdMarker = undefined;
      }
    } while (keyMarker !== undefined);

    let continuationToken: string | undefined;
    do {
      const resp = await this.s3Client.send(
        new ListObjectsV2Command({
          Bucket: bucketName,
          ContinuationToken: continuationToken,
        })
      );
      const objects = (resp.Contents ?? [])
        .map((c) => c.Key)
        .filter((k): k is string => k !== undefined);
      if (objects.length > 0) {
        await this.s3Client.send(
          new DeleteObjectsCommand({
            Bucket: bucketName,
            Delete: { Objects: objects.map((Key) => ({ Key })) },
          })
        );
      }
      continuationToken = resp.IsTruncated
        ? resp.NextContinuationToken
        : undefined;
    } while (continuationToken !== undefined);

    await this.s3Client.send(new DeleteBucketCommand({ Bucket: bucketName }));
  }
}
