import { S3Client } from '@aws-sdk/client-s3';
import { inject } from 'vitest';
import { getListFiles } from '../../lib/s3-util';
import { S3ClientHelper } from '../helpers/s3-helper';

const LOCAL_STACK_HOST = inject('localStackHost');

describe('getListFiles', () => {
  test('ListFilesWithPrefix', async () => {
    // Given:
    const s3ClientHelper = new S3ClientHelper(
      new S3Client({
        endpoint: LOCAL_STACK_HOST,
        region: 'us-east-1',
        forcePathStyle: true,
        credentials: {
          secretAccessKey: 'test',
          accessKeyId: 'test',
        },
      })
    );
    const prefix = 'tmp';
    const { bucketName } = await s3ClientHelper.setupS3({
      files: [
        {
          fileSize: 1024,
          fileCount: 5,
        },
      ],
      prefix,
    });
    const s3Client = new S3Client({
      endpoint: inject('localStackHost'),
      region: 'us-east-1',
      forcePathStyle: true,
      credentials: {
        secretAccessKey: 'test',
        accessKeyId: 'test',
      },
    });

    // When:
    const res = await getListFiles(s3Client, bucketName, prefix);

    // Then:
    expect(res).toEqual([
      {
        key: 'tmp/file-1-1.txt',
        size: 1024,
      },
      {
        key: 'tmp/file-1-2.txt',
        size: 1024,
      },
      {
        key: 'tmp/file-1-3.txt',
        size: 1024,
      },
      {
        key: 'tmp/file-1-4.txt',
        size: 1024,
      },
      {
        key: 'tmp/file-1-5.txt',
        size: 1024,
      },
    ]);
  });
});
