export class S3File {
  readonly key: string;
  readonly size: number;
  start: number;

  constructor(key: string, size: number, start: number) {
    this.key = key;
    this.size = size;
    this.start = start;
  }

  remainSize(): number {
    return this.size - this.start;
  }

  eat(consumedSize: number): void {
    this.start += consumedSize;
  }

  clone(): S3File {
    return new S3File(this.key, this.size, this.start);
  }

  withConsumed(consumedSize: number): S3File {
    return new S3File(this.key, this.size, this.start + consumedSize);
  }
}
