[![ci](https://github.com/shuntaka9576/s3-concat/actions/workflows/ci.yml/badge.svg?branch=main)](https://github.com/shuntaka9576/s3-concat/actions/workflows/ci.yml) [![codecov](https://codecov.io/gh/shuntaka9576/s3-concat/graph/badge.svg?token=ES0V32EAHO)](https://codecov.io/gh/shuntaka9576/s3-concat) [![npm version](https://img.shields.io/npm/v/s3-concat.svg)](https://www.npmjs.com/package/s3-concat) [![dependencies](https://img.shields.io/badge/dependencies-0-brightgreen)](https://www.npmjs.com/package/s3-concat?activeTab=dependencies)

# s3-concat

s3-concat is a zero-dependency library and CLI that concatenates multiple AWS S3 objects into one using multipart upload. It is ideal for managing large datasets and streamlining S3 workflows. The library automatically switches between multipart upload for files over 5 MiB and streaming for smaller files, while also allowing you to specify the concatenation order.

- Consolidate multiple output files from Athena UNLOAD, Iceberg, and ETL workflows into single S3 objects
- Merge fragmented JSONL/CSV files into larger objects while preserving data order
- Efficiently process mixed workloads with object sizes ranging from 5 MiB to 5 GiB+

## Installation

### Library

```bash
pnpm add s3-concat
```

`s3-concat` has zero runtime dependencies. `@aws-sdk/client-s3` is declared as a `peerDependency`, so the SDK version stays under your control and is not duplicated in your tree. Install it alongside `s3-concat`.

```bash
pnpm add s3-concat @aws-sdk/client-s3
```

### CLI

`s3-concat` also ships with a CLI for one-off concat jobs from the shell. Install it globally alongside the AWS SDK.

```bash
npm i -g s3-concat @aws-sdk/client-s3
# or
pnpm add -g s3-concat @aws-sdk/client-s3
```

Uses the standard AWS SDK credential chain (`AWS_REGION`, `AWS_PROFILE`, …); a reproducible demo dataset and dry-run lives in [`scripts/cli-demo.sh`](./scripts/cli-demo.sh).

Each output object is assembled via S3 multipart upload using one of two part types.

- **`UploadPartCopy`** — for source files ≥ 5 MiB. S3 copies the byte range server-side, so no bytes flow through the client. Sources larger than 5 GiB are split into 5 GiB chunks (S3's per-part copy limit).
- **`UploadPart`** — for source files < 5 MiB and any leftover tail of a copied file. Bytes are streamed through the client and coalesced with adjacent small files until each part reaches the 5 MiB minimum.

```bash
$ aws s3 ls s3://my-bucket/src/
2026-06-18 14:13:29 5370806272 a.bin
2026-06-18 14:13:29 6442450944 b.bin
2026-06-18 14:13:29    3145728 c.bin

# Single output. --dry-run prints the multipart plan without writing.
$ s3-concat \
    --src-bucket my-bucket \
    --dst-bucket my-bucket \
    --src-prefix src \
    --dst-prefix out \
    --concat-file-name merged.bin \
    --join-order keyNameAsc \
    --dry-run
dry-run: 3 source file(s), 11816402944 bytes -> 1 output object(s)

s3://my-bucket/out/merged.bin (11816402944 bytes, 5 parts)
├─ UploadPartCopy  5368709120 bytes  s3://my-bucket/src/a.bin bytes=0-5368709119
├─ UploadPart      5242880 bytes
│  ├─ s3://my-bucket/src/a.bin (2097152 bytes)
│  └─ s3://my-bucket/src/b.bin (3145728 bytes)
├─ UploadPartCopy  5368709120 bytes  s3://my-bucket/src/b.bin bytes=3145728-5371854847
├─ UploadPartCopy  1070596096 bytes  s3://my-bucket/src/b.bin bytes=5371854848-6442450943
└─ UploadPart      3145728 bytes
   └─ s3://my-bucket/src/c.bin (3145728 bytes)

# Drop --dry-run to commit.
$ s3-concat ... --concat-file-name merged.bin
wrote s3://my-bucket/out/merged.bin (11816402944 bytes)

# Split output via --concat-file-name-template + --min-size.
# {i} expands to a 1-based index per output; --json gives a machine-readable result.
$ s3-concat \
    --src-bucket my-bucket \
    --dst-bucket my-bucket \
    --src-prefix src \
    --dst-prefix out \
    --concat-file-name-template 'concat_{i}.bin' \
    --min-size 7GiB \
    --join-order keyNameAsc \
    --dry-run
dry-run: 3 source file(s), 11816402944 bytes -> 2 output object(s)

s3://my-bucket/out/concat_1.bin (11813257216 bytes, 4 parts)
├─ UploadPartCopy  5368709120 bytes  s3://my-bucket/src/a.bin bytes=0-5368709119
├─ UploadPart      5242880 bytes
│  ├─ s3://my-bucket/src/a.bin (2097152 bytes)
│  └─ s3://my-bucket/src/b.bin (3145728 bytes)
├─ UploadPartCopy  5368709120 bytes  s3://my-bucket/src/b.bin bytes=3145728-5371854847
└─ UploadPartCopy  1070596096 bytes  s3://my-bucket/src/b.bin bytes=5371854848-6442450943

s3://my-bucket/out/concat_2.bin (3145728 bytes, 1 part)
└─ UploadPart      3145728 bytes
   └─ s3://my-bucket/src/c.bin (3145728 bytes)
```

<details>
<summary><strong>Options</strong></summary>

| Option | Description |
| --- | --- |
| `--src-bucket <name>` | Source bucket name (required) |
| `--dst-bucket <name>` | Destination bucket name (required) |
| `--src-prefix <prefix>` | Source key prefix; repeat to scan multiple prefixes (required) |
| `--dst-prefix <prefix>` | Destination key prefix (required) |
| `--concat-file-name <name>` | Single output object name (mutually exclusive with template) |
| `--concat-file-name-template <t>` | Template for split outputs; must contain `{i}` |
| `--min-size <size>` | Start a new output once the current one reaches this size, e.g. `5GiB`, `100MiB`. Source files are never split across outputs — a single source always lands in one output object. |
| `--p-limit <n>` | Concurrency limit (default 5) |
| `--join-order <order>` | `fetchOrder` (default), `keyNameAsc`, or `keyNameDsc` |
| `--dry-run` | Print plan without performing the concat |
| `--verbose` | Verbose logging to stderr |
| `--json` | Emit the result as JSON on stdout |
| `-h, --help` | Show help |
| `-v, --version` | Show version |

</details>

## Usage

### Example

#### Example 1: Copying a Single Heavy Object

Pointing `s3-concat` at a single source turns it into a parallel multipart copier. Bytes move server-side via `UploadPartCopy`, and sources larger than 5 GiB (S3's per-part copy limit, and also the cap on a single `CopyObject` call) are automatically split into 5 GiB chunks uploaded in parallel — useful when the standard `CopyObject` API can't handle the object size in one shot.

```ts
import { S3Client } from '@aws-sdk/client-s3';
import { S3Concat } from 's3-concat';

const s3Client = new S3Client({});

const main = async () => {
  const s3Concat = new S3Concat({
    s3Client,
    srcBucketName: 'my-bucket',
    dstBucketName: 'my-bucket',
    dstPrefix: 'copied',
    concatFileName: 'heavy-object.bin',
  });

  await s3Concat.addFiles('path/to/heavy-object.bin');
  await s3Concat.concat();
};

main().then(() => console.log('success'));
```

`addFiles` resolves its argument as a `ListObjectsV2` prefix, so any other key starting with the same string is picked up too. When targeting a single object, make sure the key is unique under that prefix.

#### Example 2: Concatenating into a Single File

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

#### Example 3: Concatenating into Multiple Files with minSize

This example shows how to use the minSize option to split the concatenated files if the total size exceeds the specified limit. `minSize` only controls when to start a new output object — source files themselves are never split. Each source file always lands entirely inside a single output object; its bytes are never spread across two outputs, even when adding it pushes the current output well past `minSize` (for example, a 1 GiB source under `minSize: '5MiB'` produces one 1 GiB output, not 200 sliced outputs).

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

#### Example 4: Custom Join Order Example

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
for `CreateMultipartUpload` / `CompleteMultipartUpload` traffic).

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
