import { GiB, MiB } from '../../tests/helpers/value';
import { Deque } from '../std/deque';
import { S3File } from './file';

interface PartCopyTask {
  uploadType: 'PartCopy';
  s3File: S3File;
  start: number;
  end: number;
}

interface PartTask {
  uploadType: 'Part';
  s3Files: S3File[];
}

export type UploadTask = PartCopyTask | PartTask;

export const newPartCopyTask = (
  s3File: S3File,
  start: number,
  end: number
): PartCopyTask => {
  return {
    uploadType: 'PartCopy',
    s3File,
    start,
    end,
  };
};

export const newPartTask = (s3Files: S3File[]): PartTask => {
  return {
    uploadType: 'Part',
    s3Files,
  };
};

export const planedSplitFile = (
  concatFileNameCallback: (idx?: number) => string,
  s3Files: { files: Deque<S3File>; size: number },
  minValue?: number
): {
  keyName: string;
  s3Files: { files: Deque<S3File>; size: number };
}[] => {
  const splitFiles: {
    keyName: string;
    s3Files: { files: Deque<S3File>; size: number };
  }[] = [];

  let perFile: null | {
    keyName: string;
    s3Files: { files: Deque<S3File>; size: number };
  } = null;

  let perFileIdx = 1;

  if (minValue == null) {
    return [
      {
        keyName: concatFileNameCallback(perFileIdx),
        s3Files: s3Files,
      },
    ];
  }

  while (s3Files.files.size > 0) {
    const s3File = s3Files.files.popFront();
    if (s3File == null) {
      break;
    }

    if (perFile == null) {
      const perS3Files = new Deque<S3File>();

      perS3Files.pushBack(s3File);

      perFile = {
        keyName: concatFileNameCallback(perFileIdx),
        s3Files: { files: perS3Files, size: s3File.size },
      };

      if (s3File.size >= minValue) {
        splitFiles.push(perFile);

        perFileIdx += 1;
        perFile = null;
      }

      continue;
    }

    if (perFile.s3Files.size + s3File.size >= minValue) {
      perFile.s3Files.size += s3File.size;
      perFile.s3Files.files.pushBack(s3File);
      splitFiles.push(perFile);

      perFileIdx += 1;
      perFile = null;
    } else {
      perFile.s3Files.size += s3File.size;
      perFile.s3Files.files.pushBack(s3File);
    }
  }

  if (perFile != null) {
    splitFiles.push(perFile);
  }

  return splitFiles;
};

export const planedUploadTask = (s3Files: Deque<S3File>): UploadTask[] => {
  const tasks: UploadTask[] = [];
  const partUploadLimit = 5 * MiB;
  const partCopyLimit = 5 * GiB;

  while (s3Files.size > 0) {
    const file = s3Files.popFront();
    if (file == null) {
      break;
    }

    if (file.remainSize() >= partUploadLimit) {
      while (file.remainSize() >= partUploadLimit) {
        if (file.remainSize() >= partCopyLimit) {
          tasks.push(
            newPartCopyTask(
              file.clone(),
              file.start,
              file.start + partCopyLimit
            )
          );
          file.eat(partCopyLimit);
        } else {
          tasks.push(
            newPartCopyTask(
              file.clone(),
              file.start,
              file.start + file.remainSize()
            )
          );
          file.eat(file.remainSize());
        }
      }

      if (file.remainSize() > 0) {
        s3Files.pushFront(file);
      }
    } else {
      let remainSize = partUploadLimit - file.remainSize();
      const partTask = newPartTask([file.clone()]);

      while (true) {
        if (s3Files.size === 0) {
          tasks.push(partTask);
          break;
        }

        const nextFile = s3Files.popFront();

        if (nextFile == null) {
          break;
        }

        if (remainSize < nextFile.remainSize()) {
          partTask.s3Files.push(new S3File(nextFile.key, remainSize, 0));
          tasks.push(partTask);

          s3Files.pushFront(
            new S3File(nextFile.key, nextFile.size, nextFile.start + remainSize)
          );

          break;
        }

        if (remainSize === nextFile.remainSize()) {
          partTask.s3Files.push(new S3File(nextFile.key, remainSize, 0));
          tasks.push(partTask);

          break;
        }

        partTask.s3Files.push(nextFile.clone());
        remainSize -= nextFile.remainSize();

        if (remainSize === 0) {
          tasks.push(partTask);

          break;
        }
      }
    }
  }

  return tasks;
};

export const getPartSizeForPartTask = (partTask: PartTask): number => {
  let total = 0;
  for (const f of partTask.s3Files) {
    total += f.remainSize();
  }
  return total;
};
