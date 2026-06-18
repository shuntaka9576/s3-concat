import { S3Client } from '@aws-sdk/client-s3';
import { S3Concat } from '../s3-concat';
import {
  buildConcatFileNameCallback,
  type CliConfig,
  HELP_TEXT,
  parseCliArgs,
} from './parse';

declare const __S3_CONCAT_VERSION__: string;

type Stdio = {
  stdout: { write: (s: string) => void };
  stderr: { write: (s: string) => void };
};

type RunOptions = {
  argv: readonly string[];
  stdio?: Stdio;
  createS3Client?: () => unknown;
};

const defaultStdio: Stdio = {
  stdout: { write: (s) => process.stdout.write(s) },
  stderr: { write: (s) => process.stderr.write(s) },
};

const buildS3Concat = (config: CliConfig, s3Client: unknown): S3Concat => {
  // biome-ignore lint/suspicious/noExplicitAny: S3Concat accepts a structural S3Client; the AWS SDK instance satisfies it.
  const client = s3Client as any;
  const base = {
    s3Client: client,
    srcBucketName: config.srcBucket,
    dstBucketName: config.dstBucket,
    dstPrefix: config.dstPrefix,
    minSize: config.minSize,
    pLimit: config.pLimit,
    joinOrder: config.joinOrder === 'fetchOrder' ? undefined : config.joinOrder,
  } as const;

  if (config.output.kind === 'name') {
    return new S3Concat({ ...base, concatFileName: config.output.name });
  }
  return new S3Concat({
    ...base,
    concatFileNameCallback: buildConcatFileNameCallback(config.output),
  });
};

export const run = async (options: RunOptions): Promise<number> => {
  const stdio = options.stdio ?? defaultStdio;
  const result = parseCliArgs(options.argv);

  if (result.kind === 'help') {
    stdio.stdout.write(`${HELP_TEXT}\n`);
    return 0;
  }
  if (result.kind === 'version') {
    stdio.stdout.write(`${__S3_CONCAT_VERSION__}\n`);
    return 0;
  }
  if (result.kind === 'error') {
    stdio.stderr.write(`error: ${result.message}\n`);
    stdio.stderr.write('run with --help for usage.\n');
    return 1;
  }

  const config = result.config;
  const s3Client = options.createS3Client?.() ?? new S3Client({});
  const s3Concat = buildS3Concat(config, s3Client);

  for (const prefix of config.srcPrefixes) {
    if (config.verbose) {
      stdio.stderr.write(`scanning s3://${config.srcBucket}/${prefix}\n`);
    }
    await s3Concat.addFiles(prefix);
  }

  if (config.dryRun) {
    const summary = {
      kind: 'dry-run' as const,
      srcBucket: config.srcBucket,
      dstBucket: config.dstBucket,
      dstPrefix: config.dstPrefix,
      srcPrefixes: config.srcPrefixes,
      minSize: config.minSize,
      pLimit: config.pLimit,
      joinOrder: config.joinOrder,
      output: config.output,
    };
    if (config.json) {
      stdio.stdout.write(`${JSON.stringify(summary)}\n`);
    } else {
      stdio.stdout.write(
        `dry-run: would concat files from ${config.srcPrefixes
          .map((p) => `s3://${config.srcBucket}/${p}`)
          .join(', ')} into s3://${config.dstBucket}/${config.dstPrefix}/\n`
      );
    }
    return 0;
  }

  const concatResult = await s3Concat.concat();

  if (config.json) {
    stdio.stdout.write(`${JSON.stringify(concatResult)}\n`);
  } else if (concatResult.kind === 'fileNotFound') {
    stdio.stdout.write('no files matched the given prefix(es).\n');
  } else if (concatResult.kind === 'allEmpty') {
    stdio.stdout.write(
      `all ${concatResult.emptyKeys.length} matched file(s) are empty; nothing to concat.\n`
    );
  } else {
    for (const { key, size } of concatResult.keys) {
      stdio.stdout.write(
        `wrote s3://${config.dstBucket}/${key} (${size} bytes)\n`
      );
    }
    if (concatResult.skippedEmptyKeys.length > 0) {
      stdio.stdout.write(
        `skipped ${concatResult.skippedEmptyKeys.length} empty file(s).\n`
      );
    }
  }
  return 0;
};
