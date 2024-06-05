import {
  CompleteMultipartUploadCommand,
  CreateMultipartUploadCommand,
  UploadPartCommand,
  UploadPartCopyCommand,
  type UploadPartCopyCommandOutput,
} from '@aws-sdk/client-s3';
import pLimit from 'p-limit';
import * as s3Util from './s3-util';
import {
  type StorageSize,
  type StorageUnit,
  sizeToBytes,
} from './storage-size';

const MULTI_PART_UPLOAD_LIMIT = sizeToBytes('5MiB');
const DEFAULT_LIMIT_CONCURRENCY = 5;

export type S3Client = {
  // biome-ignore lint/suspicious/noExplicitAny: Using `any` type to ensure S3Client is not dependent on a specific AWS SDK version
  send(command: any): Promise<any>;
};

export type ConcatParams = {
  /**
   * The S3 client to use for operations.
   */
  s3Client: S3Client;
  /**
   * The name of the source S3 bucket (e.g., "my-source-bucket").
   * This should be the plain bucket name without any prefixes like "s3://" or "arn:".
   */
  srcBucketName: string;
  /**
   * The name of the destination S3 bucket (e.g., "my-destination-bucket").
   * This should be the plain bucket name without any prefixes like "s3://" or "arn:".
   */
  dstBucketName: string;
  /**
   * The key (path) in the destination bucket where the concatenated file will be saved.
   */
  dstPrefix: string;
  /**
   * The minimum size threshold for creating separate concatenated files.
   * If the total size of files to be concatenated is smaller than this value,
   * they will be concatenated together until this size is reached.
   * If undefined, all files will be concatenated into a single file.
   * @default undefined
   */
  minSize?: StorageSize<StorageUnit> | undefined;
  /**
   * The maximum number of concurrent asynchronous I/O operations.
   * This limits the number of parallel S3 operations to avoid overwhelming the system.
   * @default 5
   */
  pLimit?: number;
} & (
  | {
      /**
       * A callback function to generate the name of the concatenated file.
       * This function receives an optional index parameter which can be used to generate unique file names.
       */
      concatFileNameCallback: (idx?: number) => string;
      concatFileName?: never;
    }
  | {
      /**
       * The name of the concatenated file.
       * If specified, all concatenated files will use this name.
       * If minSize is specified, there is a risk of overwriting files with the same name,
       * so do not use this when there is a possibility of files being split, as there is a risk of overwriting.
       */
      concatFileNameCallback?: never;
      concatFileName: string;
    }
);

type FileUploadTask = {
  tasks: {
    multiPartUploads: {
      key: string;
      size: number;
    }[];
    localUploads: {
      key: string;
      size: number;
    }[];
    totalSize: number;
    concatKey: string;
  }[];
};

type ConcatResult =
  | {
      kind: 'concatenated';
      keys: { key: string; size: number }[];
    }
  | { kind: 'fileNotFound' };

export class S3Concat {
  private s3Client: S3Client;
  private srcBucketName: string;
  private dstBucketName: string;
  private dstKey: string;
  private allFiles: { key: string; size: number }[];
  private concatFileNameCallback: (idx?: number) => string;
  private limitConcurrency: number;
  private minSize?: number;

  constructor(params: ConcatParams) {
    this.s3Client = params.s3Client;
    this.srcBucketName = params.srcBucketName;
    this.dstBucketName = params.dstBucketName;
    this.dstKey = params.dstPrefix;
    this.allFiles = [];
    this.limitConcurrency = params.pLimit ?? DEFAULT_LIMIT_CONCURRENCY;
    this.minSize = params.minSize && sizeToBytes(params.minSize);

    if (typeof params.concatFileName === 'string') {
      this.concatFileNameCallback = () => params.concatFileName;
    } else {
      this.concatFileNameCallback = params.concatFileNameCallback;
    }
  }

  async addFiles(prefix: string): Promise<void> {
    const files = await s3Util.getListFiles(
      this.s3Client,
      this.srcBucketName,
      prefix
    );
    this.allFiles.push(...files);
  }

  async concat(): Promise<ConcatResult> {
    const uploadTasks = this.createFileUploadTasks(
      this.allFiles,
      (i: number) => `${this.dstKey}/${this.concatFileNameCallback(i)}`
    );

    if (uploadTasks.tasks.length === 0) {
      return { kind: 'fileNotFound' };
    }

    const limit = pLimit(this.limitConcurrency);

    const results = await Promise.allSettled(
      uploadTasks.tasks.map((task) => limit(() => this.uploadTask(task)))
    );

    results.map((result) => {
      if (result.status !== 'fulfilled') {
        throw new Error(`concat task error: ${result.reason}`);
      }
    });

    const concatenatedKeys = uploadTasks.tasks.map((task) => {
      return {
        key: task.concatKey,
        size: task.totalSize,
      };
    });

    return {
      kind: 'concatenated',
      keys: concatenatedKeys,
    };
  }

  private async uploadTask(task: {
    multiPartUploads: { key: string; size: number }[];
    localUploads: { key: string; size: number }[];
    totalSize: number;
    concatKey: string;
  }): Promise<void> {
    const createUploadResponse = await this.s3Client.send(
      new CreateMultipartUploadCommand({
        Bucket: this.dstBucketName,
        Key: task.concatKey,
      })
    );

    const uploadId = createUploadResponse.UploadId;
    const parts: { PartNumber: number; ETag: string }[] = [];
    const limit = pLimit(this.limitConcurrency);

    const s3EtagParts: { PartNumber: number; ETag: string }[][] =
      await Promise.all(
        task.multiPartUploads.map((s3Part, i) => {
          return limit(() => {
            const partSize = Math.min(sizeToBytes('5GiB'), s3Part.size);
            const partCount = Math.ceil(s3Part.size / partSize);
            const copyPromises = [];

            for (let partNumber = 1; partNumber <= partCount; partNumber++) {
              const start = (partNumber - 1) * partSize;
              const end = Math.min(start + partSize - 1, s3Part.size - 1);
              const copyRange = `bytes=${start}-${end}`;

              copyPromises.push(
                new Promise<{ PartNumber: number; ETag: string }>(
                  (resolve, reject) => {
                    this.s3Client
                      .send(
                        new UploadPartCopyCommand({
                          CopySource: `${this.srcBucketName}/${s3Part.key}`,
                          CopySourceRange: copyRange,
                          Bucket: this.dstBucketName,
                          Key: task.concatKey,
                          UploadId: uploadId,
                          PartNumber: partNumber + i * partCount,
                        })
                      )
                      .then((res: UploadPartCopyCommandOutput) => {
                        if (res.CopyPartResult?.ETag == null) {
                          throw new Error(
                            'Unexpected error: ETag is missing in the UploadPartCopyCommand response.'
                          );
                        }
                        resolve({
                          PartNumber: partNumber + i * partCount,
                          ETag: res.CopyPartResult.ETag,
                        });
                      })
                      .catch((error) => {
                        reject(
                          new Error(
                            `Failed to upload part ${partNumber} ${error}`
                          )
                        );
                      });
                  }
                )
              );
            }

            return Promise.all(copyPromises);
          });
        })
      );

    parts.push(...s3EtagParts.flat());

    let posPartNumber = parts.length + 1;
    const partSize = sizeToBytes('10MiB');
    let buffer = Buffer.alloc(0);

    for (const localPart of task.localUploads) {
      const partStream = await s3Util.getStream(
        this.s3Client,
        this.srcBucketName,
        localPart.key
      );

      for await (const chunk of partStream) {
        buffer = Buffer.concat([buffer, chunk]);

        while (buffer.length >= partSize) {
          const partBuffer = buffer.subarray(0, partSize);
          buffer = buffer.subarray(partSize);

          const uploadPartCommand = new UploadPartCommand({
            Bucket: this.dstBucketName,
            Key: task.concatKey,
            UploadId: uploadId,
            PartNumber: posPartNumber,
            Body: partBuffer,
          });

          const uploadPartResponse =
            await this.s3Client.send(uploadPartCommand);
          parts.push({
            ETag: uploadPartResponse.ETag,
            PartNumber: posPartNumber,
          });

          posPartNumber += 1;
        }
      }
    }

    if (buffer.length > 0) {
      const uploadPartCommand = new UploadPartCommand({
        Bucket: this.dstBucketName,
        Key: task.concatKey,
        UploadId: uploadId,
        PartNumber: posPartNumber,
        Body: buffer,
      });

      const uploadPartResponse = await this.s3Client.send(uploadPartCommand);
      parts.push({
        ETag: uploadPartResponse.ETag,
        PartNumber: posPartNumber,
      });
    }

    const sortedParts = parts.sort((a, b) => a.PartNumber - b.PartNumber);
    await this.s3Client.send(
      new CompleteMultipartUploadCommand({
        Bucket: this.dstBucketName,
        Key: task.concatKey,
        UploadId: uploadId,
        MultipartUpload: {
          Parts: sortedParts,
        },
      })
    );
  }

  private createFileUploadTasks(
    files: { key: string; size: number }[],
    keyCallback: (idx: number) => string
  ): FileUploadTask {
    const { groups, currentGroup } = files.reduce<{
      groups: { key: string; size: number }[][];
      currentGroup: { key: string; size: number }[];
      accSize: number;
    }>(
      (acc, file) => {
        acc.currentGroup.push(file);
        acc.accSize += file.size;

        if (this.minSize != null) {
          if (acc.accSize >= this.minSize) {
            acc.groups.push([...acc.currentGroup]);
            acc.currentGroup = [];
            acc.accSize = 0;
          }
          return acc;
        }
        return acc;
      },
      { groups: [], currentGroup: [], accSize: 0 }
    );

    if (currentGroup.length > 0) {
      groups.push(currentGroup);
    }

    return {
      tasks: groups.map((group, index) => ({
        multiPartUploads: group.filter(
          (file) => file.size >= MULTI_PART_UPLOAD_LIMIT
        ),
        localUploads: group.filter(
          (file) => file.size < MULTI_PART_UPLOAD_LIMIT
        ),
        totalSize: group.reduce((sum, file) => sum + file.size, 0),
        concatKey: keyCallback(index + 1),
      })),
    };
  }
}
