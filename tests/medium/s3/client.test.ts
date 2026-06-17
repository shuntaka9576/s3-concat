import { inject } from 'vitest';
import { getListFiles } from '../../../lib/s3/client';
import { createTestS3Client } from '../../helpers/s3-client-factory';
import { S3ClientHelper } from '../../helpers/s3-helper';

const TEST_S3_CONFIG = inject('testS3Config');

describe('getListFiles', () => {
  test('ListFilesWithPrefix', async () => {
    // Given:
    const s3Client = createTestS3Client(TEST_S3_CONFIG);
    const s3ClientHelper = new S3ClientHelper(s3Client);
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

    // When:
    const res = await getListFiles(s3Client, bucketName, prefix);

    // Then:
    expect(res).toEqual([
      {
        key: 'tmp/file-1-1.txt',
        size: 1024,
        lastModified: expect.any(Date),
      },
      {
        key: 'tmp/file-1-2.txt',
        size: 1024,
        lastModified: expect.any(Date),
      },
      {
        key: 'tmp/file-1-3.txt',
        size: 1024,
        lastModified: expect.any(Date),
      },
      {
        key: 'tmp/file-1-4.txt',
        size: 1024,
        lastModified: expect.any(Date),
      },
      {
        key: 'tmp/file-1-5.txt',
        size: 1024,
        lastModified: expect.any(Date),
      },
    ]);
  });
});
