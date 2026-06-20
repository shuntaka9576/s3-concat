import {
  DeleteObjectCommand,
  GetObjectCommand,
  ListObjectsV2Command,
} from '@aws-sdk/client-s3';
import { inject } from 'vitest';
import { type Plan, S3Concat } from '../../lib/s3-concat';
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

  test('CrossBucketConcat', async () => {
    // Given: files in a src bucket, an empty dst bucket. The mix of <5 MiB
    // and >=5 MiB inputs exercises both the streaming GetObject path and the
    // UploadPartCopy path — both used to read from dst instead of src.
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
    const { bucketName: srcBucketName } = await s3ClientHelper.setupS3({
      files,
      prefix,
    });
    const { bucketName: dstBucketName } =
      await s3ClientHelper.createEmptyBucket();

    const s3Concat = new S3Concat({
      s3Client,
      srcBucketName,
      dstBucketName,
      dstPrefix,
      concatFileName,
    });
    await s3Concat.addFiles(prefix);

    // When:
    const result = await s3Concat.concat();

    // Then: concatenated object lands in the dst bucket.
    expect(result).toEqual({
      keys: [
        {
          key: `${dstPrefix}/${concatFileName}`,
          size: 1000 * KiB * 11 + 5 * MiB * 3,
        },
      ],
      kind: 'concatenated',
      skippedEmptyKeys: [],
    });
    const dstListing = await s3Client.send(
      new ListObjectsV2Command({
        Bucket: dstBucketName,
        Prefix: dstPrefix,
      })
    );
    expect(dstListing.Contents).toEqual([
      expect.objectContaining({
        ETag: expect.any(String),
        Key: `${dstPrefix}/${concatFileName}`,
        LastModified: expect.any(Date),
        Size: 1000 * KiB * 11 + 5 * MiB * 3,
        StorageClass: 'STANDARD',
      }),
    ]);

    // And: src bucket is untouched — no output/ prefix appeared on src.
    const srcDstPrefixListing = await s3Client.send(
      new ListObjectsV2Command({
        Bucket: srcBucketName,
        Prefix: dstPrefix,
      })
    );
    expect(srcDstPrefixListing.Contents ?? []).toEqual([]);
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

describe('S3Concat.executePlan', () => {
  describe('when replaying a plan through JSON round-trip', () => {
    test('writes a destination object equal to the literal expected bytes', async () => {
      // Given: source has 2 files of 1024000 bytes 'A' and 1 file of 5242880
      // bytes 'B'. keyNameAsc places them as file-1-1, file-1-2, file-2-1.
      const files = [
        { fileSize: 1024000, fileCount: 2, fill: 'A' },
        { fileSize: 5242880, fileCount: 1, fill: 'B' },
      ];
      const prefix = 'tmp';
      const dstPrefix = 'output';
      const s3Client = createTestS3Client(TEST_S3_CONFIG);
      const s3ClientHelper = new S3ClientHelper(s3Client);
      const { bucketName } = await s3ClientHelper.setupS3({ files, prefix });

      const s3Concat = new S3Concat({
        s3Client,
        srcBucketName: bucketName,
        dstBucketName: bucketName,
        dstPrefix,
        concatFileName: 'replayed.bin',
        joinOrder: 'keyNameAsc',
      });
      await s3Concat.addFiles(prefix);

      // When: plan -> JSON.stringify/parse -> executePlan.
      const planResult = s3Concat.plan();
      if (planResult.kind !== 'planned') throw new Error('expected plan');
      const restored = JSON.parse(JSON.stringify(planResult)) as Plan;
      const executed = await S3Concat.executePlan(restored, { s3Client });

      // Then: result + destination bytes match literal expected (2 * 1024000
      // 'A' followed by 5242880 'B' = 7290880 bytes total).
      expect(executed).toEqual([
        { key: `${dstPrefix}/replayed.bin`, size: 7290880 },
      ]);
      const got = await s3Client.send(
        new GetObjectCommand({
          Bucket: bucketName,
          Key: `${dstPrefix}/replayed.bin`,
        })
      );
      const body = await got.Body?.transformToByteArray();
      const expected = Buffer.concat([
        Buffer.alloc(2048000, 'A'),
        Buffer.alloc(5242880, 'B'),
      ]);
      expect(body).toEqual(new Uint8Array(expected));
    });
  });

  describe('when a Part source has rangeStart > 0 (post-PartCopy tail)', () => {
    test('reads from the recorded offset, not from byte 0', async () => {
      // Given: a 10 MiB source object made of 5242880 'A' then 5242880 'B',
      // and a hand-crafted plan whose only Part references that source at
      // rangeStart=5242880 for bytes=5242880. (This is the shape concat()
      // produces after a PartCopy eats a file's head; constructed directly
      // here so the test does not need a 5 GiB+ object.)
      const prefix = 'tmp';
      const dstPrefix = 'output';
      const sourceKey = `${prefix}/half-half.bin`;
      const outputKey = `${dstPrefix}/tail.bin`;
      const s3Client = createTestS3Client(TEST_S3_CONFIG);
      const s3ClientHelper = new S3ClientHelper(s3Client);
      const { bucketName } = await s3ClientHelper.setupS3({
        files: [],
        prefix,
      });
      await s3ClientHelper.uploadFile(
        bucketName,
        sourceKey,
        Buffer.concat([Buffer.alloc(5242880, 'A'), Buffer.alloc(5242880, 'B')])
      );

      const plan: Plan = {
        kind: 'planned',
        srcBucketName: bucketName,
        dstBucketName: bucketName,
        totalFiles: 1,
        totalBytes: 5242880,
        skippedEmptyKeys: [],
        outputs: [
          {
            key: outputKey,
            size: 5242880,
            parts: [
              {
                kind: 'Part',
                partNumber: 1,
                size: 5242880,
                sources: [
                  { key: sourceKey, rangeStart: 5242880, bytes: 5242880 },
                ],
              },
            ],
          },
        ],
      };

      // When: executePlan replays the plan.
      await S3Concat.executePlan(plan, { s3Client });

      // Then: destination is 5242880 bytes of 'B' (the second half). If
      // rangeStart were ignored the output would be all 'A'.
      const got = await s3Client.send(
        new GetObjectCommand({ Bucket: bucketName, Key: outputKey })
      );
      const body = await got.Body?.transformToByteArray();
      expect(body).toEqual(new Uint8Array(Buffer.alloc(5242880, 'B')));
    });
  });

  describe('with check: true', () => {
    describe('when a referenced source has been deleted since plan() ran', () => {
      test('rejects with a missing diagnostic well under the streaming-body hang window', async () => {
        // Given: small files force the streaming Part path (sources < 5 MiB).
        // Without check:true a missing source would hang the GetObject ->
        // UploadPart pipeline for ~60s before throwing.
        const files = [{ fileSize: 1000 * KiB, fileCount: 3 }];
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
          concatFileName: 'merged.bin',
        });
        await s3Concat.addFiles(prefix);
        const planResult = s3Concat.plan();
        if (planResult.kind !== 'planned') throw new Error('expected plan');
        const victimKey = `${prefix}/file-1-2.txt`;
        await s3Client.send(
          new DeleteObjectCommand({ Bucket: bucketName, Key: victimKey })
        );

        // When: executePlan with check:true runs after the source is deleted.
        const start = performance.now();
        const result = expect(
          S3Concat.executePlan(planResult, { s3Client, check: true })
        ).rejects.toThrow(
          new RegExp(`missing s3://${bucketName}/${victimKey}`)
        );
        await result;
        const elapsed = performance.now() - start;

        // Then: rejection arrives via pre-flight HeadObject, not via the
        // streaming-body timeout — i.e. fast enough to be safe inside a
        // Lambda runtime.
        expect(elapsed).toBeLessThan(10_000);
      });
    });

    describe('when a referenced source has been truncated since plan() ran', () => {
      test('rejects with a truncated diagnostic', async () => {
        // Given: a 6 MiB source overwritten with a 1 KiB stub so the plan
        // refers to byte ranges that no longer exist.
        const files = [{ fileSize: 6 * MiB, fileCount: 2 }];
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
          concatFileName: 'merged.bin',
        });
        await s3Concat.addFiles(prefix);
        const planResult = s3Concat.plan();
        if (planResult.kind !== 'planned') throw new Error('expected plan');
        const victimKey = `${prefix}/file-1-1.txt`;
        await s3ClientHelper.uploadFile(
          bucketName,
          victimKey,
          Buffer.alloc(1 * KiB, '0')
        );

        // When: executePlan with check:true runs against the truncated source.
        // Then: pre-flight detects the shrinkage and rejects.
        await expect(
          S3Concat.executePlan(planResult, { s3Client, check: true })
        ).rejects.toThrow(/truncated/);
      });
    });
  });
});
