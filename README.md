[![ci](https://github.com/shuntaka9576/s3-concat/actions/workflows/ci.yml/badge.svg?branch=main)](https://github.com/shuntaka9576/s3-concat/actions/workflows/ci.yml) [![codecov](https://codecov.io/gh/shuntaka9576/s3-concat/graph/badge.svg?token=ES0V32EAHO)](https://codecov.io/gh/shuntaka9576/s3-concat)

# s3-concat

`s3-concat` is a library for concatenating multiple files stored in AWS S3 into a single file using multipart upload. This is particularly useful for handling large datasets and optimizing S3 operations. Files larger than 5MiB are uploaded using multipart upload, while files smaller than 5MiB are concatenated via streaming.

Inspired by the [s3-concat](https://pypi.org/project/s3-concat/) project on PyPI.

## Installation

```bash
npm install s3-concat
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

## License

This project is licensed under the MIT License.

## Contributing

Contributions are welcome! Please open an issue or submit a pull request with any changes or improvements.
