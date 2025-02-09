import pLimit from 'p-limit';
import * as s3Client from './s3/client';
import { S3File } from './s3/file';
import {
  type UploadTask,
  planedSplitFile as planedSplitFiles,
  planedUploadTask,
} from './s3/task';
import { Deque } from './std/deque';
import {
  type StorageSize,
  type StorageUnit,
  sizeToBytes,
} from './std/storage-size';

const DEFAULT_LIMIT_CONCURRENCY = 5;

export type S3Client = {
  // biome-ignore lint/suspicious/noExplicitAny: Using `any` type to ensure S3Client is not dependent on a specific AWS SDK version
  send(command: any): Promise<any>;
};

type BuiltinJoinOrderCompareFnSpecifier = 'keyNameAsc' | 'keyNameDsc';

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
   * A callback function or preset value to determine the order in which files are concatenated.
   *
   * When provided as a callback, it receives the file name and its creation timestamp as arguments.
   * The function should return a value (either a number or a string) that represents the sort order,
   * where lower values indicate a higher priority in the concatenation sequence.
   *
   * Alternatively, you can specify the string `'fetchOrder'` to use a built-in default ordering
   * that is optimized for performance.
   *
   * @default fetchOrder
   */
  joinOrder?:
    | JoinOrderCompareFn<{
        key: string;
        size: number;
        lastModified: Date;
      }>
    | 'fetchOrder'
    | BuiltinJoinOrderCompareFnSpecifier;
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

type ConcatResult =
  | {
      kind: 'concatenated';
      keys: { key: string; size: number }[];
    }
  | { kind: 'fileNotFound' };

type JoinOrderCompareFn<T> = (a: T, b: T) => number;

const builtinJoinOrderFunc = (
  specifier: BuiltinJoinOrderCompareFnSpecifier
): ((
  a: { key: string; size: number; lastModified: Date },
  b: { key: string; size: number; lastModified: Date }
) => number) => {
  switch (specifier) {
    case 'keyNameAsc':
      return (a, b) =>
        a.key.localeCompare(b.key, undefined, {
          numeric: true,
          sensitivity: 'base',
        });
    case 'keyNameDsc':
      return (a, b) =>
        b.key.localeCompare(a.key, undefined, {
          numeric: true,
          sensitivity: 'base',
        });
    default:
      throw new Error(`unknown joinOrder specifier: ${specifier}`);
  }
};

export class S3Concat {
  private s3Client: S3Client;
  private srcBucketName: string;
  private dstBucketName: string;
  private dstKey: string;
  private s3Files: { key: string; size: number; lastModified: Date }[];
  private concatFileNameCallback: (idx?: number) => string;
  private joinOrder:
    | JoinOrderCompareFn<{
        key: string;
        size: number;
        lastModified: Date;
      }>
    | 'fetchOrder';
  private limitConcurrency: number;
  private minSize?: number;

  constructor(params: ConcatParams) {
    this.s3Client = params.s3Client;
    this.srcBucketName = params.srcBucketName;
    this.dstBucketName = params.dstBucketName;
    this.dstKey = params.dstPrefix;
    this.s3Files = [];
    this.limitConcurrency = params.pLimit ?? DEFAULT_LIMIT_CONCURRENCY;
    this.minSize = params.minSize && sizeToBytes(params.minSize);
    this.joinOrder = ((
      joinOrder:
        | BuiltinJoinOrderCompareFnSpecifier
        | 'fetchOrder'
        | JoinOrderCompareFn<{
            key: string;
            size: number;
            lastModified: Date;
          }>
        | undefined
    ) => {
      if (joinOrder === 'fetchOrder' || joinOrder == null) {
        return 'fetchOrder';
      }

      if (typeof joinOrder === 'function') {
        return joinOrder;
      }

      return builtinJoinOrderFunc(joinOrder);
    })(params.joinOrder);

    if (typeof params.concatFileName === 'string') {
      this.concatFileNameCallback = () => params.concatFileName;
    } else {
      this.concatFileNameCallback = params.concatFileNameCallback;
    }
  }

  async addFiles(prefix: string): Promise<void> {
    const files = await s3Client.getListFiles(
      this.s3Client,
      this.srcBucketName,
      prefix
    );

    this.s3Files.push(...files);
  }

  private toS3Files(): { files: Deque<S3File>; size: number } {
    if (this.joinOrder !== 'fetchOrder') {
      this.s3Files = this.s3Files.sort(this.joinOrder);
    }

    const s3Files = new Deque<S3File>();
    let size = 0;
    for (const s3File of this.s3Files) {
      s3Files.pushBack(new S3File(s3File.key, s3File.size, 0));
      size += s3File.size;
    }

    return { files: s3Files, size };
  }

  async concat(): Promise<ConcatResult> {
    const s3Files = this.toS3Files();
    const splitFiles = planedSplitFiles(
      this.concatFileNameCallback,
      s3Files,
      this.minSize
    );

    const splitFileAndUploadTask: {
      keyName: string;
      uploadTasks: UploadTask[];
      size: number;
    }[] = splitFiles.map((splitFile) => {
      const uploadTasks = planedUploadTask(splitFile.s3Files.files);

      return {
        keyName: splitFile.keyName,
        uploadTasks,
        size: splitFile.s3Files.size,
      };
    });

    if (splitFileAndUploadTask.length === 0) {
      return { kind: 'fileNotFound' };
    }

    const limit = pLimit(this.limitConcurrency);

    const keys = await Promise.all(
      splitFileAndUploadTask.map((task) =>
        limit(async () => {
          const key = `${this.dstKey}/${task.keyName}`;
          await s3Client.concatWithMultipartUpload(
            this.s3Client,
            this.dstBucketName,
            key,
            task.uploadTasks,
            this.limitConcurrency
          );
          return {
            key,
            size: task.size,
          };
        })
      )
    );

    return {
      kind: 'concatenated',
      keys: keys,
    };
  }
}
