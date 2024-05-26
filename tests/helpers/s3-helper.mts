import { randomUUID } from 'node:crypto';
import {
  CreateBucketCommand,
  PutObjectCommand,
  type S3Client,
} from '@aws-sdk/client-s3';
import pLimit from 'p-limit';

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
    }[];
    prefix: string;
  }): Promise<{ bucketName: string }> {
    const bucketName = `bucket-${randomUUID()}`;

    await this.s3Client.send(
      new CreateBucketCommand({
        Bucket: bucketName,
      })
    );

    const promises = params.files.flatMap((file, i) => {
      const fileContent = Buffer.alloc(file.fileSize, '0');

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
}
