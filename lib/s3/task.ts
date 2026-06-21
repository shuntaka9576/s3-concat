import { Deque } from '../std/deque';
import { GiB, MiB } from '../std/storage-size';
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
): PartCopyTask => ({
  uploadType: 'PartCopy',
  s3File,
  start,
  end,
});

export const newPartTask = (s3Files: S3File[]): PartTask => ({
  uploadType: 'Part',
  s3Files,
});

type SplitGroup = {
  keyName: string;
  s3Files: { files: Deque<S3File>; size: number };
};

export const plannedSplitFiles = (
  concatFileNameCallback: (idx?: number) => string,
  s3Files: { files: Deque<S3File>; size: number },
  minValue?: number
): SplitGroup[] => {
  if (minValue === undefined) {
    return [
      {
        keyName: concatFileNameCallback(1),
        s3Files,
      },
    ];
  }

  const splitFiles: SplitGroup[] = [];
  let perFile: SplitGroup | undefined;
  let perFileIdx = 1;

  while (s3Files.files.size > 0) {
    const s3File = s3Files.files.popFront();
    if (s3File === undefined) {
      break;
    }

    if (perFile === undefined) {
      const perS3Files = new Deque<S3File>();
      perS3Files.pushBack(s3File);

      perFile = {
        keyName: concatFileNameCallback(perFileIdx),
        s3Files: { files: perS3Files, size: s3File.size },
      };

      if (s3File.size >= minValue) {
        splitFiles.push(perFile);
        perFileIdx += 1;
        perFile = undefined;
      }
      continue;
    }

    perFile.s3Files.size += s3File.size;
    perFile.s3Files.files.pushBack(s3File);

    if (perFile.s3Files.size >= minValue) {
      splitFiles.push(perFile);
      perFileIdx += 1;
      perFile = undefined;
    }
  }

  if (perFile !== undefined) {
    splitFiles.push(perFile);
  }

  return splitFiles;
};

const PART_UPLOAD_LIMIT = 5 * MiB;
const PART_COPY_LIMIT = 5 * GiB;

const planLargeFileCopies = (
  tasks: UploadTask[],
  file: S3File,
  remainingFiles: Deque<S3File>
): void => {
  while (file.remainSize() >= PART_UPLOAD_LIMIT) {
    const chunk = Math.min(file.remainSize(), PART_COPY_LIMIT);
    tasks.push(newPartCopyTask(file.clone(), file.start, file.start + chunk));
    file.eat(chunk);
  }

  if (file.remainSize() > 0) {
    remainingFiles.pushFront(file);
  }
};

const planSmallFilePart = (
  tasks: UploadTask[],
  firstFile: S3File,
  remainingFiles: Deque<S3File>
): void => {
  let remainSize = PART_UPLOAD_LIMIT - firstFile.remainSize();
  const partTask = newPartTask([firstFile.clone()]);

  while (remainSize > 0) {
    const nextFile = remainingFiles.popFront();
    if (nextFile === undefined) {
      tasks.push(partTask);
      return;
    }

    if (remainSize < nextFile.remainSize()) {
      partTask.s3Files.push(new S3File(nextFile.key, remainSize, 0));
      tasks.push(partTask);
      remainingFiles.pushFront(
        new S3File(nextFile.key, nextFile.size, nextFile.start + remainSize)
      );
      return;
    }

    if (remainSize === nextFile.remainSize()) {
      partTask.s3Files.push(new S3File(nextFile.key, remainSize, 0));
      tasks.push(partTask);
      return;
    }

    partTask.s3Files.push(nextFile.clone());
    remainSize -= nextFile.remainSize();
  }

  tasks.push(partTask);
};

export const plannedUploadTasks = (s3Files: Deque<S3File>): UploadTask[] => {
  const tasks: UploadTask[] = [];

  while (s3Files.size > 0) {
    const file = s3Files.popFront();
    if (file === undefined) {
      break;
    }

    if (file.remainSize() >= PART_UPLOAD_LIMIT) {
      planLargeFileCopies(tasks, file, s3Files);
    } else {
      planSmallFilePart(tasks, file, s3Files);
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
