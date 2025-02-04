import {
  GetObjectCommand,
  ListObjectsV2Command,
  S3Client,
} from '@aws-sdk/client-s3';
import { inject } from 'vitest';
import { S3Concat } from '../../lib/s3-concat';
import { S3ClientHelper } from '../helpers/s3-helper';
import { KiB, MiB } from '../helpers/value';

const LOCAL_STACK_HOST = inject('localStackHost');

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
    const { bucketName } = await s3ClientHelper.setupS3({
      files,
      prefix,
    });
    const s3Client = new S3Client({
      endpoint: LOCAL_STACK_HOST,
      region: 'us-east-1',
      forcePathStyle: true,
      credentials: {
        secretAccessKey: 'test',
        accessKeyId: 'test',
      },
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
    });
    const got = await s3Client.send(
      new ListObjectsV2Command({
        Bucket: bucketName,
        Prefix: dstPrefix,
      })
    );
    expect(got.Contents).toEqual([
      {
        ETag: expect.any(String),
        Key: `${dstPrefix}/${concatFileName}`,
        LastModified: expect.any(Date),
        Size: 1000 * KiB * 11 + 5 * MiB * 3,
        StorageClass: 'STANDARD',
      },
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
    const { bucketName } = await s3ClientHelper.setupS3({
      files,
      prefix,
    });

    const dstPrefix = 'output';
    const s3Client = new S3Client({
      endpoint: LOCAL_STACK_HOST,
      region: 'us-east-1',
      forcePathStyle: true,
      credentials: {
        secretAccessKey: 'test',
        accessKeyId: 'test',
      },
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
    });
    const got = await s3Client.send(
      new ListObjectsV2Command({
        Bucket: bucketName,
        Prefix: dstPrefix,
      })
    );
    expect(got.Contents).toEqual([
      {
        Key: 'output/concat_1.json',
        LastModified: expect.any(Date),
        ETag: expect.any(String),
        Size: 3 * KiB,
        StorageClass: 'STANDARD',
      },
      {
        Key: 'output/concat_2.json',
        LastModified: expect.any(Date),
        ETag: expect.any(String),
        Size: 3 * KiB,
        StorageClass: 'STANDARD',
      },
      {
        Key: 'output/concat_3.json',
        LastModified: expect.any(Date),
        ETag: expect.any(String),
        Size: 3 * KiB,
        StorageClass: 'STANDARD',
      },
      {
        Key: 'output/concat_4.json',
        LastModified: expect.any(Date),
        ETag: expect.any(String),
        Size: 1 * KiB + 5 * MiB,
        StorageClass: 'STANDARD',
      },
      {
        Key: 'output/concat_5.json',
        LastModified: expect.any(Date),
        ETag: expect.any(String),
        Size: 5 * MiB,
        StorageClass: 'STANDARD',
      },
      {
        Key: 'output/concat_6.json',
        LastModified: expect.any(Date),
        ETag: expect.any(String),
        Size: 5 * MiB,
        StorageClass: 'STANDARD',
      },
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
      const { bucketName } = await s3ClientHelper.setupS3({
        files,
        prefix,
      });

      const dstPrefix = 'output';
      const s3Client = new S3Client({
        endpoint: LOCAL_STACK_HOST,
        region: 'us-east-1',
        forcePathStyle: true,
        credentials: {
          secretAccessKey: 'test',
          accessKeyId: 'test',
        },
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
      });

      const got = await s3Client.send(
        new ListObjectsV2Command({
          Bucket: bucketName,
          Prefix: dstPrefix,
        })
      );
      expect(got.Contents).toEqual([
        {
          Key: 'output/concat_1.json',
          LastModified: expect.any(Date),
          ETag: expect.any(String),
          Size: 8,
          StorageClass: 'STANDARD',
        },
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
    const { bucketName } = await s3ClientHelper.setupS3({
      files,
      prefix,
    });

    const dstPrefix = 'output';
    const s3Client = new S3Client({
      endpoint: LOCAL_STACK_HOST,
      region: 'us-east-1',
      forcePathStyle: true,
      credentials: {
        secretAccessKey: 'test',
        accessKeyId: 'test',
      },
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
    });
    const got = await s3Client.send(
      new ListObjectsV2Command({
        Bucket: bucketName,
        Prefix: dstPrefix,
      })
    );
    expect(got.Contents).toEqual([
      {
        Key: 'output/concat_1.json',
        LastModified: expect.any(Date),
        ETag: expect.any(String),
        Size: 8,
        StorageClass: 'STANDARD',
      },
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
    const { bucketName } = await s3ClientHelper.setupS3({
      files: [],
      prefix,
    });

    // When:
    const s3Client = new S3Client({
      endpoint: LOCAL_STACK_HOST,
      region: 'us-east-1',
      forcePathStyle: true,
      credentials: {
        secretAccessKey: 'test',
        accessKeyId: 'test',
      },
    });
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
});
