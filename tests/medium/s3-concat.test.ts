import { GetObjectCommand, ListObjectsV2Command } from '@aws-sdk/client-s3';
import { inject } from 'vitest';
import { S3Concat } from '../../lib/s3-concat';
import { KiB, MiB } from '../../lib/std/storage-size';
import { createTestS3Client } from '../helpers/s3-client-factory';
import { S3ClientHelper } from '../helpers/s3-helper';

const TEST_S3_CONFIG = inject('testS3Config');

describe('concat', () => {
  test('SingleFileOutputWithoutMinSize', async () => {
    // Given:
    const files = [
      {
        fileSize: 1000 * KiB,
        fileCount: 11,
      },
      {
        fileSize: 5 * MiB,
        fileCount: 3,
      },
    ];
    const prefix = 'tmp';
    const dstPrefix = 'output';
    const concatFileName = 'output.json';
    const s3Client = createTestS3Client(TEST_S3_CONFIG);
    const s3ClientHelper = new S3ClientHelper(s3Client);
    const { bucketName } = await s3ClientHelper.setupS3({
      files,
      prefix,
    });
    const s3Concat = new S3Concat({
      s3Client,
      srcBucketName: bucketName,
      dstBucketName: bucketName,
      dstPrefix,
      concatFileName,
    });
    await s3Concat.addFiles(prefix);

    // When:
    const result = await s3Concat.concat();

    // Then:
    expect(result).toEqual({
      keys: [
        {
          key: 'output/output.json',
          size: 26992640,
        },
      ],
      kind: 'concatenated',
      skippedEmptyKeys: [],
    });
    const got = await s3Client.send(
      new ListObjectsV2Command({
        Bucket: bucketName,
        Prefix: dstPrefix,
      })
    );
    expect(got.Contents).toEqual([
      expect.objectContaining({
        ETag: expect.any(String),
        Key: `${dstPrefix}/${concatFileName}`,
        LastModified: expect.any(Date),
        Size: 1000 * KiB * 11 + 5 * MiB * 3,
        StorageClass: 'STANDARD',
      }),
    ]);
  });

  test('SplitAndMergeWithMinSize', async () => {
    // Given:
    const files = [
      {
        fileSize: 1 * KiB,
        fileCount: 10,
      },
      {
        fileSize: 5 * MiB,
        fileCount: 3,
      },
    ];
    const prefix = 'tmp';
    const dstPrefix = 'output';
    const s3Client = createTestS3Client(TEST_S3_CONFIG);
    const s3ClientHelper = new S3ClientHelper(s3Client);
    const { bucketName } = await s3ClientHelper.setupS3({
      files,
      prefix,
    });
    const s3Concat = new S3Concat({
      s3Client,
      srcBucketName: bucketName,
      dstBucketName: bucketName,
      dstPrefix,
      minSize: '3KiB',
      concatFileNameCallback: (idx) => `concat_${idx}.json`,
    });
    await s3Concat.addFiles(prefix);

    // When:
    const result = await s3Concat.concat();

    // Then:
    expect(result).toEqual({
      keys: [
        {
          key: 'output/concat_1.json',
          size: 3072,
        },
        {
          key: 'output/concat_2.json',
          size: 3072,
        },
        {
          key: 'output/concat_3.json',
          size: 3072,
        },
        {
          key: 'output/concat_4.json',
          size: 5243904,
        },
        {
          key: 'output/concat_5.json',
          size: 5242880,
        },
        {
          key: 'output/concat_6.json',
          size: 5242880,
        },
      ],
      kind: 'concatenated',
      skippedEmptyKeys: [],
    });
    const got = await s3Client.send(
      new ListObjectsV2Command({
        Bucket: bucketName,
        Prefix: dstPrefix,
      })
    );
    expect(got.Contents).toEqual([
      expect.objectContaining({
        Key: 'output/concat_1.json',
        LastModified: expect.any(Date),
        ETag: expect.any(String),
        Size: 3 * KiB,
        StorageClass: 'STANDARD',
      }),
      expect.objectContaining({
        Key: 'output/concat_2.json',
        LastModified: expect.any(Date),
        ETag: expect.any(String),
        Size: 3 * KiB,
        StorageClass: 'STANDARD',
      }),
      expect.objectContaining({
        Key: 'output/concat_3.json',
        LastModified: expect.any(Date),
        ETag: expect.any(String),
        Size: 3 * KiB,
        StorageClass: 'STANDARD',
      }),
      expect.objectContaining({
        Key: 'output/concat_4.json',
        LastModified: expect.any(Date),
        ETag: expect.any(String),
        Size: 1 * KiB + 5 * MiB,
        StorageClass: 'STANDARD',
      }),
      expect.objectContaining({
        Key: 'output/concat_5.json',
        LastModified: expect.any(Date),
        ETag: expect.any(String),
        Size: 5 * MiB,
        StorageClass: 'STANDARD',
      }),
      expect.objectContaining({
        Key: 'output/concat_6.json',
        LastModified: expect.any(Date),
        ETag: expect.any(String),
        Size: 5 * MiB,
        StorageClass: 'STANDARD',
      }),
    ]);
  });

  describe.each([
    {
      name: 'KeyNameAscConcat',
      joinOrder: 'keyNameAsc' as const,
      expectedBody: '01111122',
    },
    {
      name: 'KeyNameDscConcat',
      joinOrder: 'keyNameDsc' as const,
      expectedBody: '22111110',
    },
  ])('$name', ({ joinOrder, expectedBody }) => {
    test('concat test', async () => {
      const files = [
        {
          fileSize: 1,
          fileCount: 1,
          fill: '0',
        },
        {
          fileSize: 5,
          fileCount: 1,
          fill: '1',
        },
        {
          fileSize: 1,
          fileCount: 2,
          fill: '2',
        },
      ];
      const prefix = 'tmp';
      const dstPrefix = 'output';
      const s3Client = createTestS3Client(TEST_S3_CONFIG);
      const s3ClientHelper = new S3ClientHelper(s3Client);
      const { bucketName } = await s3ClientHelper.setupS3({
        files,
        prefix,
      });

      const s3Concat = new S3Concat({
        s3Client,
        srcBucketName: bucketName,
        dstBucketName: bucketName,
        dstPrefix,
        joinOrder,
        concatFileNameCallback: (idx) => `concat_${idx}.json`,
      });
      await s3Concat.addFiles(prefix);

      const result = await s3Concat.concat();

      expect(result).toEqual({
        keys: [
          {
            key: 'output/concat_1.json',
            size: 8,
          },
        ],
        kind: 'concatenated',
        skippedEmptyKeys: [],
      });

      const got = await s3Client.send(
        new ListObjectsV2Command({
          Bucket: bucketName,
          Prefix: dstPrefix,
        })
      );
      expect(got.Contents).toEqual([
        expect.objectContaining({
          Key: 'output/concat_1.json',
          LastModified: expect.any(Date),
          ETag: expect.any(String),
          Size: 8,
          StorageClass: 'STANDARD',
        }),
      ]);

      const fileContent = await s3Client.send(
        new GetObjectCommand({
          Bucket: bucketName,
          Key: 'output/concat_1.json',
        })
      );
      const bodyContents = await fileContent.Body?.transformToString();
      expect(bodyContents).toBe(expectedBody);
    });
  });

  test('CustomJoinOrderConcat', async () => {
    const files = [
      {
        fileSize: 1,
        fileCount: 1,
        fill: '0',
      },
      {
        fileSize: 5,
        fileCount: 1,
        fill: '1',
      },
      {
        fileSize: 2,
        fileCount: 1,
        fill: '2',
      },
    ];
    const prefix = 'tmp';
    const dstPrefix = 'output';
    const s3Client = createTestS3Client(TEST_S3_CONFIG);
    const s3ClientHelper = new S3ClientHelper(s3Client);
    const { bucketName } = await s3ClientHelper.setupS3({
      files,
      prefix,
    });

    const s3Concat = new S3Concat({
      s3Client,
      srcBucketName: bucketName,
      dstBucketName: bucketName,
      dstPrefix,
      joinOrder: (a, b) => b.size - a.size,
      concatFileNameCallback: (idx) => `concat_${idx}.json`,
    });
    await s3Concat.addFiles(prefix);

    // When:
    const result = await s3Concat.concat();

    // Then:
    expect(result).toEqual({
      keys: [
        {
          key: 'output/concat_1.json',
          size: 8,
        },
      ],
      kind: 'concatenated',
      skippedEmptyKeys: [],
    });
    const got = await s3Client.send(
      new ListObjectsV2Command({
        Bucket: bucketName,
        Prefix: dstPrefix,
      })
    );
    expect(got.Contents).toEqual([
      expect.objectContaining({
        Key: 'output/concat_1.json',
        LastModified: expect.any(Date),
        ETag: expect.any(String),
        Size: 8,
        StorageClass: 'STANDARD',
      }),
    ]);

    const fileContent = await s3Client.send(
      new GetObjectCommand({
        Bucket: bucketName,
        Key: 'output/concat_1.json',
      })
    );
    const bodyContents = await fileContent.Body?.transformToString();
    expect('11111220').toEqual(bodyContents);
  });

  test('NotFoundFile', async () => {
    // Given:
    const prefix = 'tmp';
    const dstPrefix = 'output';
    const s3Client = createTestS3Client(TEST_S3_CONFIG);
    const s3ClientHelper = new S3ClientHelper(s3Client);
    const { bucketName } = await s3ClientHelper.setupS3({
      files: [],
      prefix,
    });

    // When:
    const s3Concat = new S3Concat({
      s3Client,
      srcBucketName: bucketName,
      dstBucketName: bucketName,
      dstPrefix,
      minSize: '3KiB',
      concatFileNameCallback: (idx) => `concat_${idx}.json`,
    });
    const result = await s3Concat.concat();

    // Then:
    expect(result).toEqual({
      kind: 'fileNotFound',
    });
  });

  test('NotFoundFileWithoutMinSize', async () => {
    // Given:
    const prefix = 'tmp/does-not-exist';
    const dstPrefix = 'output';
    const s3Client = createTestS3Client(TEST_S3_CONFIG);
    const s3ClientHelper = new S3ClientHelper(s3Client);
    const { bucketName } = await s3ClientHelper.setupS3({
      files: [],
      prefix,
    });

    // When:
    const s3Concat = new S3Concat({
      s3Client,
      srcBucketName: bucketName,
      dstBucketName: bucketName,
      dstPrefix,
      concatFileName: 'never.txt',
    });
    await s3Concat.addFiles(prefix);
    const result = await s3Concat.concat();

    // Then:
    expect(result).toEqual({
      kind: 'fileNotFound',
    });
  });

  test('AllEmpty', async () => {
    // Given:
    const prefix = 'tmp';
    const dstPrefix = 'output';
    const s3Client = createTestS3Client(TEST_S3_CONFIG);
    const s3ClientHelper = new S3ClientHelper(s3Client);
    const { bucketName } = await s3ClientHelper.setupS3({
      files: [
        {
          fileSize: 0,
          fileCount: 3,
        },
      ],
      prefix,
    });

    // When:
    const s3Concat = new S3Concat({
      s3Client,
      srcBucketName: bucketName,
      dstBucketName: bucketName,
      dstPrefix,
      concatFileName: 'never.txt',
    });
    await s3Concat.addFiles(prefix);
    const result = await s3Concat.concat();

    // Then:
    expect(result).toEqual({
      kind: 'allEmpty',
      emptyKeys: expect.arrayContaining([
        `${prefix}/file-1-1.txt`,
        `${prefix}/file-1-2.txt`,
        `${prefix}/file-1-3.txt`,
      ]),
    });
    if (result.kind === 'allEmpty') {
      expect(result.emptyKeys).toHaveLength(3);
    }
  });

  test('MixedWithEmpty', async () => {
    // Given:
    const prefix = 'tmp';
    const dstPrefix = 'output';
    const concatFileName = 'output.json';
    const s3Client = createTestS3Client(TEST_S3_CONFIG);
    const s3ClientHelper = new S3ClientHelper(s3Client);
    const { bucketName } = await s3ClientHelper.setupS3({
      files: [
        {
          fileSize: 1000 * KiB,
          fileCount: 2,
        },
        {
          fileSize: 0,
          fileCount: 2,
        },
      ],
      prefix,
    });

    // When:
    const s3Concat = new S3Concat({
      s3Client,
      srcBucketName: bucketName,
      dstBucketName: bucketName,
      dstPrefix,
      concatFileName,
    });
    await s3Concat.addFiles(prefix);
    const result = await s3Concat.concat();

    // Then:
    expect(result).toEqual({
      kind: 'concatenated',
      keys: [
        {
          key: `${dstPrefix}/${concatFileName}`,
          size: 1000 * KiB * 2,
        },
      ],
      skippedEmptyKeys: expect.arrayContaining([
        `${prefix}/file-2-1.txt`,
        `${prefix}/file-2-2.txt`,
      ]),
    });
    if (result.kind === 'concatenated') {
      expect(result.skippedEmptyKeys).toHaveLength(2);
    }
    const got = await s3Client.send(
      new ListObjectsV2Command({
        Bucket: bucketName,
        Prefix: dstPrefix,
      })
    );
    expect(got.Contents).toEqual([
      expect.objectContaining({
        Key: `${dstPrefix}/${concatFileName}`,
        LastModified: expect.any(Date),
        ETag: expect.any(String),
        Size: 1000 * KiB * 2,
        StorageClass: 'STANDARD',
      }),
    ]);
  });
});
