import { parseArgs } from 'node:util';
import type { StorageSize, StorageUnit } from '../std/storage-size';

export type JoinOrderName = 'keyNameAsc' | 'keyNameDsc' | 'fetchOrder';

export type CliConfig = {
  srcBucket: string;
  dstBucket: string;
  srcPrefixes: string[];
  dstPrefix: string;
  output:
    | { kind: 'name'; name: string }
    | { kind: 'template'; template: string };
  minSize?: StorageSize<StorageUnit>;
  pLimit?: number;
  joinOrder: JoinOrderName;
  dryRun: boolean;
  verbose: boolean;
  json: boolean;
};

export type ParseResult =
  | { kind: 'run'; config: CliConfig }
  | { kind: 'help' }
  | { kind: 'version' }
  | { kind: 'error'; message: string };

const JOIN_ORDERS: readonly JoinOrderName[] = [
  'keyNameAsc',
  'keyNameDsc',
  'fetchOrder',
];

const STORAGE_SIZE_REGEX = /^\d+(B|KiB|MiB|GiB|TiB)$/;

const isJoinOrder = (v: string): v is JoinOrderName =>
  (JOIN_ORDERS as readonly string[]).includes(v);

const isStorageSize = (v: string): v is StorageSize<StorageUnit> =>
  STORAGE_SIZE_REGEX.test(v);

const OPTIONS = {
  'src-bucket': { type: 'string' },
  'dst-bucket': { type: 'string' },
  'src-prefix': { type: 'string', multiple: true },
  'dst-prefix': { type: 'string' },
  'concat-file-name': { type: 'string' },
  'concat-file-name-template': { type: 'string' },
  'min-size': { type: 'string' },
  'p-limit': { type: 'string' },
  'join-order': { type: 'string' },
  'dry-run': { type: 'boolean' },
  verbose: { type: 'boolean' },
  json: { type: 'boolean' },
  help: { type: 'boolean', short: 'h' },
  version: { type: 'boolean', short: 'v' },
} as const;

export const HELP_TEXT = `Usage: s3-concat [options]

Concatenate multiple S3 objects into one (or more) S3 objects using multipart upload.

Required:
  --src-bucket <name>                 Source bucket name
  --dst-bucket <name>                 Destination bucket name
  --src-prefix <prefix>               Source key prefix to scan (repeatable)
  --dst-prefix <prefix>               Destination key prefix for output objects

  Exactly one of:
    --concat-file-name <name>         Single output object name
    --concat-file-name-template <t>   Template for split outputs. {i} is replaced
                                      with a 1-based index (e.g., concat_{i}.json)

Optional:
  --min-size <size>                   Split output once this size is reached.
                                      Format: <number><unit>, unit in B|KiB|MiB|GiB|TiB
  --p-limit <n>                       Concurrency limit (default 5)
  --join-order <order>                fetchOrder | keyNameAsc | keyNameDsc
                                      (default fetchOrder)
  --dry-run                           Print plan without running concat
  --verbose                           Verbose logging
  --json                              Emit machine-readable JSON result
  -h, --help                          Show this help and exit
  -v, --version                       Show version and exit

AWS credentials/region are resolved by the AWS SDK via the standard
environment (AWS_REGION, AWS_PROFILE, ~/.aws/config, IMDS, ...).
`;

export const parseCliArgs = (argv: readonly string[]): ParseResult => {
  let values: ReturnType<typeof parseArgs>['values'];
  try {
    values = parseArgs({
      args: [...argv],
      options: OPTIONS,
      strict: true,
      allowPositionals: false,
    }).values;
  } catch (err) {
    return {
      kind: 'error',
      message: err instanceof Error ? err.message : String(err),
    };
  }

  if (values.help) {
    return { kind: 'help' };
  }
  if (values.version) {
    return { kind: 'version' };
  }

  const srcBucket = values['src-bucket'] as string | undefined;
  const dstBucket = values['dst-bucket'] as string | undefined;
  const dstPrefix = values['dst-prefix'] as string | undefined;
  const srcPrefixes = values['src-prefix'] as string[] | undefined;

  const missing: string[] = [];
  if (!srcBucket) missing.push('--src-bucket');
  if (!dstBucket) missing.push('--dst-bucket');
  if (!dstPrefix) missing.push('--dst-prefix');
  if (!srcPrefixes || srcPrefixes.length === 0) missing.push('--src-prefix');
  if (missing.length > 0) {
    return {
      kind: 'error',
      message: `missing required option(s): ${missing.join(', ')}`,
    };
  }

  const name = values['concat-file-name'] as string | undefined;
  const template = values['concat-file-name-template'] as string | undefined;
  if (name && template) {
    return {
      kind: 'error',
      message:
        '--concat-file-name and --concat-file-name-template are mutually exclusive',
    };
  }
  if (!name && !template) {
    return {
      kind: 'error',
      message:
        'one of --concat-file-name or --concat-file-name-template is required',
    };
  }
  if (template && !template.includes('{i}')) {
    return {
      kind: 'error',
      message: '--concat-file-name-template must contain "{i}" placeholder',
    };
  }

  const joinOrderRaw =
    (values['join-order'] as string | undefined) ?? 'fetchOrder';
  if (!isJoinOrder(joinOrderRaw)) {
    return {
      kind: 'error',
      message: `--join-order must be one of: ${JOIN_ORDERS.join(', ')}`,
    };
  }

  const minSizeRaw = values['min-size'] as string | undefined;
  let minSize: StorageSize<StorageUnit> | undefined;
  if (minSizeRaw !== undefined) {
    if (!isStorageSize(minSizeRaw)) {
      return {
        kind: 'error',
        message:
          '--min-size must match <number><unit> where unit is B, KiB, MiB, GiB, or TiB',
      };
    }
    minSize = minSizeRaw;
  }

  const pLimitRaw = values['p-limit'] as string | undefined;
  let pLimit: number | undefined;
  if (pLimitRaw !== undefined) {
    const n = Number(pLimitRaw);
    if (!Number.isInteger(n) || n <= 0) {
      return {
        kind: 'error',
        message: '--p-limit must be a positive integer',
      };
    }
    pLimit = n;
  }

  const output: CliConfig['output'] = name
    ? { kind: 'name', name }
    : { kind: 'template', template: template as string };

  return {
    kind: 'run',
    config: {
      srcBucket: srcBucket as string,
      dstBucket: dstBucket as string,
      srcPrefixes: srcPrefixes as string[],
      dstPrefix: dstPrefix as string,
      output,
      minSize,
      pLimit,
      joinOrder: joinOrderRaw,
      dryRun: Boolean(values['dry-run']),
      verbose: Boolean(values.verbose),
      json: Boolean(values.json),
    },
  };
};

export const buildConcatFileNameCallback = (
  output: CliConfig['output']
): ((i?: number) => string) => {
  if (output.kind === 'name') {
    return () => output.name;
  }
  const t = output.template;
  return (i?: number) => t.replace(/\{i\}/g, String(i ?? 1));
};
