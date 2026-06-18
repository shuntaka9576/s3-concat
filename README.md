[![ci](https://github.com/shuntaka9576/s3-concat/actions/workflows/ci.yml/badge.svg?branch=main)](https://github.com/shuntaka9576/s3-concat/actions/workflows/ci.yml) [![codecov](https://codecov.io/gh/shuntaka9576/s3-concat/graph/badge.svg?token=ES0V32EAHO)](https://codecov.io/gh/shuntaka9576/s3-concat) [![npm version](https://img.shields.io/npm/v/s3-concat.svg)](https://www.npmjs.com/package/s3-concat) [![dependencies](https://img.shields.io/badge/dependencies-0-brightgreen)](https://www.npmjs.com/package/s3-concat?activeTab=dependencies)

# s3-concat

`s3-concat` is a library for concatenating multiple files stored in AWS S3 into a single file using multipart upload. This is particularly useful for handling large datasets and optimizing S3 operations. Files larger than 5MiB are uploaded using multipart upload, while files smaller than 5MiB are concatenated via streaming. Additionally, the order in which the source files are concatenated can also be controlled.

`s3-concat` has zero runtime dependencies. `@aws-sdk/client-s3` is declared as a `peerDependency`, so the SDK version stays under your control and is not duplicated in your tree. Install it alongside `s3-concat`:

```bash
pnpm add s3-concat @aws-sdk/client-s3
```

## Installation

```bash
pnpm add s3-concat
```

## Usage

### Example

#### Example 1: Concatenating into a Single File

This example shows how to concatenate all files into a single file without using the minSize option.

```ts
import { S3Client } from '@aws-sdk/client-s3';
import { S3Concat } from 's3-concat';

const s3Client = new S3Client({});
const srcBucketName = process.env.srcBucketName!;
const dstBucketName = process.env.dstBucketName!;
const dstPrefix = 'output';

const main = async () => {
  const s3Concat = new S3Concat({
    s3Client,
    srcBucketName: srcBucketName,
    dstBucketName: dstBucketName,
    dstPrefix,
    concatFileName: 'final_concat.json',
  });

  await s3Concat.addFiles('tmp/1gb');
  await s3Concat.concat();
};

main().then(() => console.log('success'));
```

In this example, all files from the tmp/1gb prefix in the source bucket will be concatenated into a single file named final_concat.json.

#### Example 2: Concatenating into Multiple Files with minSize

This example shows how to use the minSize option to split the concatenated files if the total size exceeds the specified limit.

```ts
import { S3Client } from '@aws-sdk/client-s3';
import { S3Concat } from 's3-concat';

const s3Client = new S3Client({});
const srcBucketName = process.env.srcBucketName!;
const dstBucketName = process.env.dstBucketName!;
const dstPrefix = 'output';

const main = async () => {
  const s3Concat = new S3Concat({
    s3Client,
    srcBucketName: srcBucketName,
    dstBucketName: dstBucketName,
    dstPrefix,
    concatFileNameCallback: (i) => `concat_${i}.json`,
    minSize: '5GiB',
  });

  await s3Concat.addFiles('tmp/1gb');
  await s3Concat.concat();
};

main().then(() => console.log('success'));
```

In this example, files from the tmp/1gb prefix in the source bucket will be concatenated and split into multiple files if the total size exceeds 5GiB. The concatenated files will be named using the callback function, resulting in names like concat_1.json, concat_2.json, etc.

#### Example 3: Custom Join Order Example

It is possible to specify the join order using the joinOrder option. Although the presets keyNameDsc and keyNameAsc are supported, you can also customize the join order by providing your own function that conforms to the type JoinOrderCompareFn<T> (e.g., JoinOrderCompareFn<{ key: string; size: number; lastModified: Date }>).

```diff
// Descending order by keyName
const s3Concat = new S3Concat({
  s3Client,
  srcBucketName: srcBucketName,
  dstBucketName: dstBucketName,
  dstPrefix,
  concatFileNameCallback: (i) => `concat_${i}.json`,
+ joinOrder: 'keyNameDsc', // use builtin keyword
});

// Descending order by lastModified
const s3Concat = new S3Concat({
  s3Client,
  srcBucketName: srcBucketName,
  dstBucketName: dstBucketName,
  dstPrefix,
  concatFileNameCallback: (i) => `concat_${i}.json`,
+ joinOrder: (a, b) => a.lastModified.getTime() - b.lastModified.getTime(),
});

// Descending order by size
const s3Concat = new S3Concat({
  s3Client,
  srcBucketName: srcBucketName,
  dstBucketName: dstBucketName,
  dstPrefix,
  concatFileNameCallback: (i) => `concat_${i}.json`,
+ joinOrder: (a, b) => b.size - a.size,
});
```


## CLI

<details>
<summary><strong>Show usage and options</strong></summary>

`s3-concat` also ships with a CLI for one-off concat jobs from the shell. Install it globally alongside the AWS SDK.

```bash
npm i -g s3-concat @aws-sdk/client-s3
```

AWS credentials and region are resolved by the AWS SDK using the standard environment (`AWS_REGION`, `AWS_PROFILE`, `~/.aws/config`, IMDS, ...).

### Usage

```bash
s3-concat \
  --src-bucket my-source-bucket \
  --src-prefix tmp/1gb \
  --dst-bucket my-destination-bucket \
  --dst-prefix output \
  --concat-file-name final_concat.json
```

**Split by minimum size**

```bash
s3-concat \
  --src-bucket my-source-bucket \
  --src-prefix tmp/1gb \
  --dst-bucket my-destination-bucket \
  --dst-prefix output \
  --concat-file-name-template 'concat_{i}.json' \
  --min-size 5GiB
```

`{i}` is replaced with a 1-based index for each split output.

**Options**

| Option | Description |
| --- | --- |
| `--src-bucket <name>` | Source bucket name (required) |
| `--dst-bucket <name>` | Destination bucket name (required) |
| `--src-prefix <prefix>` | Source key prefix; repeat to scan multiple prefixes (required) |
| `--dst-prefix <prefix>` | Destination key prefix (required) |
| `--concat-file-name <name>` | Single output object name (mutually exclusive with template) |
| `--concat-file-name-template <t>` | Template for split outputs; must contain `{i}` |
| `--min-size <size>` | Split once an output reaches this size, e.g. `5GiB`, `100MiB` |
| `--p-limit <n>` | Concurrency limit (default 5) |
| `--join-order <order>` | `fetchOrder` (default), `keyNameAsc`, or `keyNameDsc` |
| `--dry-run` | Print plan without performing the concat |
| `--verbose` | Verbose logging to stderr |
| `--json` | Emit the result as JSON on stdout |
| `-h, --help` | Show help |
| `-v, --version` | Show version |

**JSON output**

Pipe the result into `jq` or any structured-output consumer.

```bash
s3-concat ... --json | jq '.keys[].key'
```

</details>

## Performance Tuning

### `pLimit`

`pLimit` is the total in-flight S3 I/O budget shared across every output
file (one global semaphore). It caps the combined count of in-flight
`UploadPart`, `UploadPartCopy`, and `GetObject` calls. The default is `5`.

Increase it for high-throughput workloads (many parts per output, fast
network); keep it low on memory-constrained runtimes such as small Lambda
functions, where each in-flight upload part also pins ~64 KiB of stream
buffer in memory.

### Socket pool (`maxSockets`)

> **Required when `pLimit ≥ 10`.** Each active part holds one `GetObject`
> and one `UploadPart` socket concurrently. If `maxSockets ≤ pLimit`,
> `UploadPart` requests queue inside the SDK and Smithy eventually aborts
> them with a non-retryable streaming error that surfaces as
> `AbortError: This operation was aborted`.

`s3-concat` does not construct an `S3Client` for you, so the HTTP socket
pool is yours to size. The default `NodeHttpHandler` keeps `maxSockets`
at the Node `http.Agent` default (`50`).

Set `maxSockets ≥ pLimit × 2` (each part needs 2 sockets, plus headroom
for `CreateMultipartUpload` / `CompleteMultipartUpload` traffic):

```ts
import { Agent as HttpsAgent } from 'node:https';
import { NodeHttpHandler } from '@smithy/node-http-handler';
import { S3Client } from '@aws-sdk/client-s3';

const pLimit = 50;
const s3Client = new S3Client({
  requestHandler: new NodeHttpHandler({
    httpsAgent: new HttpsAgent({
      maxSockets: pLimit * 2,
      keepAlive: true,
    }),
  }),
});
```

### Memory characteristics

`UploadPart` streams source bytes through a 64 KiB coalescing buffer
instead of materializing each part as one Buffer. In-flight memory scales
as `O(pLimit × 64 KiB)` rather than `O(pLimit × 5 MiB × parts_per_output)`,
which keeps tens-of-thousands-of-small-files workloads inside a 1 GiB
Lambda envelope.

## License

This project is licensed under the MIT License.

## Contributing

Contributions are welcome! Please open an issue or submit a pull request with any changes or improvements.
