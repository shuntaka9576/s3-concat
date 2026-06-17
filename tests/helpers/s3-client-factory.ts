import { S3Client } from '@aws-sdk/client-s3';
import type { FlociConfig } from '../medium/setup/global-setup';

export const createTestS3Client = (config: FlociConfig): S3Client =>
  new S3Client({
    endpoint: config.endpoint,
    region: config.region,
    forcePathStyle: true,
    credentials: {
      accessKeyId: config.accessKeyId,
      secretAccessKey: config.secretAccessKey,
    },
  });
