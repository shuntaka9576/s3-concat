import { S3Client } from '@aws-sdk/client-s3';
import type { TestS3Config } from '../medium/setup/global-setup';

export const createTestS3Client = (config: TestS3Config): S3Client => {
  if (config.mode === 'aws') {
    // Use the ambient AWS SDK credential chain (env vars, IMDS, aws-vault, ...).
    return new S3Client({ region: config.region });
  }
  return new S3Client({
    endpoint: config.endpoint,
    region: config.region,
    forcePathStyle: true,
    credentials: {
      accessKeyId: config.accessKeyId,
      secretAccessKey: config.secretAccessKey,
    },
  });
};
