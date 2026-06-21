import * as s3Client from './s3/client';
import { S3File } from './s3/file';
import {
  newPartCopyTask,
  newPartTask,
  plannedSplitFiles,
  plannedUploadTasks,
  type UploadTask,
} from './s3/task';
import pLimit, { type LimitFunction } from './std/concurrency';
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

type S3FileMeta = { key: string; size: number; lastModified: Date };

type JoinOrderCompareFn<T> = (a: T, b: T) => number;

type ResolvedJoinOrder = JoinOrderCompareFn<S3FileMeta> | 'fetchOrder';

type ConcatResult =
  | {
      kind: 'concatenated';
      keys: { key: string; size: number }[];
      skippedEmptyKeys: string[];
    }
  | { kind: 'fileNotFound' }
  | { kind: 'allEmpty'; emptyKeys: string[] };

export type PlannedPart =
  | {
      kind: 'PartCopy';
      partNumber: number;
      size: number;
      source: { key: string; rangeStart: number; rangeEnd: number };
    }
  | {
      kind: 'Part';
      partNumber: number;
      size: number;
      /**
       * Sources read sequentially via `GetObject` and coalesced into one
       * upload. `rangeStart` is the byte offset within the source; for most
       * sources it is `0`, but the first source of a Part right after a
       * `PartCopy` ate the file's head will have a non-zero `rangeStart`.
       */
      sources: { key: string; rangeStart: number; bytes: number }[];
    };

export type PlannedOutput = {
  key: string;
  size: number;
  parts: PlannedPart[];
};

export type Plan = {
  kind: 'planned';
  srcBucketName: string;
  dstBucketName: string;
  totalFiles: number;
  totalBytes: number;
  skippedEmptyKeys: string[];
  outputs: PlannedOutput[];
};

export type PlanResult =
  | Plan
  | { kind: 'fileNotFound' }
  | { kind: 'allEmpty'; emptyKeys: string[] };

export type ExecutePlanParams = {
  /**
   * The S3 client to use for operations.
   */
  s3Client: S3Client;
  /**
   * The maximum number of concurrent asynchronous I/O operations.
   * @default 5
   */
  pLimit?: number;
  /**
   * When `true`, pre-flight `HeadObject` every source key referenced by the
   * plan and verify each object still has at least the byte range the plan
   * needs. If any key is missing or has shrunk, the call rejects with a
   * drift error before any multipart upload is created.
   *
   * When `false` (the default), `executePlan` skips the check and dispatches
   * uploads immediately. The plan is assumed to be a faithful snapshot of
   * the source bucket — if a referenced object was deleted or truncated
   * since `plan()` ran, behavior depends on the part type: `UploadPartCopy`
   * surfaces the S3 error promptly, but a streaming `Part` (sources < 5 MiB
   * or trailing bytes of larger files) may hang up to the SDK's stream
   * timeout and surface an unhandled rejection.
   *
   * Set this to `true` for any flow where the plan→execute gap is
   * non-trivial (Step Functions Wait state, Slack approval, source buckets
   * with lifecycle rules).
   *
   * @default false
   */
  check?: boolean;
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
    | JoinOrderCompareFn<S3FileMeta>
    | 'fetchOrder'
    | BuiltinJoinOrderCompareFnSpecifier;
  /**
   * The maximum number of concurrent asynchronous I/O operations.
   * This limits the total in-flight `UploadPart` / `UploadPartCopy` /
   * `GetObject` calls across all output files (one shared semaphore).
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

const builtinJoinOrderFunc = (
  specifier: BuiltinJoinOrderCompareFnSpecifier
): JoinOrderCompareFn<S3FileMeta> => {
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

const resolveJoinOrder = (
  joinOrder: ConcatParams['joinOrder']
): ResolvedJoinOrder => {
  if (joinOrder === undefined || joinOrder === 'fetchOrder') {
    return 'fetchOrder';
  }
  if (typeof joinOrder === 'function') {
    return joinOrder;
  }
  return builtinJoinOrderFunc(joinOrder);
};

export class S3Concat {
  private s3Client: S3Client;
  private srcBucketName: string;
  private dstBucketName: string;
  private dstKey: string;
  private s3Files: S3FileMeta[];
  private concatFileNameCallback: (idx?: number) => string;
  private joinOrder: ResolvedJoinOrder;
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
    this.joinOrder = resolveJoinOrder(params.joinOrder);

    this.concatFileNameCallback =
      typeof params.concatFileName === 'string'
        ? () => params.concatFileName
        : params.concatFileNameCallback;
  }

  async addFiles(prefix: string): Promise<void> {
    const files = await s3Client.getListFiles(
      this.s3Client,
      this.srcBucketName,
      prefix
    );

    this.s3Files.push(...files);
  }

  private toS3Files(entries: S3FileMeta[]): {
    files: Deque<S3File>;
    size: number;
  } {
    const sorted =
      this.joinOrder === 'fetchOrder'
        ? entries
        : [...entries].sort(this.joinOrder);

    const s3Files = new Deque<S3File>();
    let size = 0;
    for (const s3File of sorted) {
      s3Files.pushBack(new S3File(s3File.key, s3File.size, 0));
      size += s3File.size;
    }

    return { files: s3Files, size };
  }

  plan(): PlanResult {
    if (this.s3Files.length === 0) {
      return { kind: 'fileNotFound' };
    }

    const emptyKeys: string[] = [];
    const nonEmpty: S3FileMeta[] = [];
    let totalBytes = 0;
    for (const f of this.s3Files) {
      if (f.size === 0) {
        emptyKeys.push(f.key);
      } else {
        nonEmpty.push(f);
        totalBytes += f.size;
      }
    }

    if (nonEmpty.length === 0) {
      return { kind: 'allEmpty', emptyKeys };
    }

    const s3Files = this.toS3Files(nonEmpty);
    const splitFiles = plannedSplitFiles(
      this.concatFileNameCallback,
      s3Files,
      this.minSize
    );

    const outputs: PlannedOutput[] = splitFiles.map((sf) => {
      const tasks = plannedUploadTasks(sf.s3Files.files);
      const parts: PlannedPart[] = tasks.map((task, idx) => {
        const partNumber = idx + 1;
        if (task.uploadType === 'PartCopy') {
          return {
            kind: 'PartCopy',
            partNumber,
            size: task.end - task.start,
            source: {
              key: task.s3File.key,
              rangeStart: task.start,
              rangeEnd: task.end,
            },
          };
        }
        const sources = task.s3Files.map((f) => ({
          key: f.key,
          rangeStart: f.start,
          bytes: f.remainSize(),
        }));
        const size = sources.reduce((acc, s) => acc + s.bytes, 0);
        return { kind: 'Part', partNumber, size, sources };
      });
      return {
        key: `${this.dstKey}/${sf.keyName}`,
        size: sf.s3Files.size,
        parts,
      };
    });

    return {
      kind: 'planned',
      srcBucketName: this.srcBucketName,
      dstBucketName: this.dstBucketName,
      totalFiles: nonEmpty.length,
      totalBytes,
      skippedEmptyKeys: emptyKeys,
      outputs,
    };
  }

  async concat(): Promise<ConcatResult> {
    const planResult = this.plan();
    if (planResult.kind !== 'planned') {
      return planResult;
    }

    const keys = await S3Concat.executePlan(planResult, {
      s3Client: this.s3Client,
      pLimit: this.limitConcurrency,
    });

    return {
      kind: 'concatenated',
      keys,
      skippedEmptyKeys: planResult.skippedEmptyKeys,
    };
  }

  /**
   * Execute a previously computed {@link Plan} without re-listing source
   * objects. The plan is self-contained — `srcBucketName` and `dstBucketName`
   * are read from the plan itself, and the source keys / byte ranges encoded
   * in `parts` are used as-is.
   *
   * Source drift is **not** validated upfront. If a source key has been
   * deleted or its size changed since {@link S3Concat.plan} ran, the underlying
   * S3 API call (`UploadPartCopy` / `GetObject`) throws and the error is
   * propagated. Decide on retry / re-plan at the caller.
   *
   * The destination keys are taken verbatim from `plan.outputs[].key` —
   * `dstPrefix` is already baked in.
   */
  static async executePlan(
    plan: Plan,
    params: ExecutePlanParams
  ): Promise<{ key: string; size: number }[]> {
    const limitConcurrency = params.pLimit ?? DEFAULT_LIMIT_CONCURRENCY;
    // One shared semaphore caps the total in-flight S3 I/O across every
    // output file (same design as `concat()`).
    const limit = pLimit(limitConcurrency);

    if (params.check === true) {
      await verifyPlanSources(
        params.s3Client,
        plan.srcBucketName,
        collectRequiredBytes(plan),
        limit
      );
    }

    return Promise.all(
      plan.outputs.map(async (output) => {
        const uploadTasks = output.parts.map(toUploadTask);
        await s3Client.concatWithMultipartUpload(
          params.s3Client,
          plan.srcBucketName,
          plan.dstBucketName,
          output.key,
          uploadTasks,
          limit
        );
        return { key: output.key, size: output.size };
      })
    );
  }
}

const collectRequiredBytes = (plan: Plan): Map<string, number> => {
  const required = new Map<string, number>();
  const bump = (key: string, value: number): void => {
    const prev = required.get(key) ?? 0;
    if (value > prev) required.set(key, value);
  };
  for (const output of plan.outputs) {
    for (const part of output.parts) {
      if (part.kind === 'PartCopy') {
        bump(part.source.key, part.source.rangeEnd);
      } else {
        for (const s of part.sources) {
          bump(s.key, s.rangeStart + s.bytes);
        }
      }
    }
  }
  return required;
};

const verifyPlanSources = async (
  client: S3Client,
  bucket: string,
  required: Map<string, number>,
  limit: LimitFunction
): Promise<void> => {
  const drift: string[] = [];
  await Promise.all(
    [...required.entries()].map(([key, requiredBytes]) =>
      limit(async () => {
        const size = await s3Client.headObjectSize(client, bucket, key);
        if (size === undefined) {
          drift.push(`missing s3://${bucket}/${key}`);
        } else if (size < requiredBytes) {
          drift.push(
            `truncated s3://${bucket}/${key} (size ${size}, plan needs ${requiredBytes})`
          );
        }
      })
    )
  );
  if (drift.length > 0) {
    throw new Error(`plan drift detected: ${drift.join('; ')}`);
  }
};

const toUploadTask = (part: PlannedPart): UploadTask => {
  if (part.kind === 'PartCopy') {
    // S3File.size is only consulted by code that walks remainSize(); for
    // PartCopy the executor only reads .key / .start / .end, so any size that
    // satisfies `start <= size` works. Use the range end to keep invariants.
    return newPartCopyTask(
      new S3File(part.source.key, part.source.rangeEnd, part.source.rangeStart),
      part.source.rangeStart,
      part.source.rangeEnd
    );
  }
  // buildMergedBody reads `bytes=${start}-${size - 1}`, so size must equal
  // `start + bytes` to drain exactly `bytes` from the source.
  return newPartTask(
    part.sources.map(
      (s) => new S3File(s.key, s.rangeStart + s.bytes, s.rangeStart)
    )
  );
};
