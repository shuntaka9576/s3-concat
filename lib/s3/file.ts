export class S3File {
  key: string;
  size: number;
  start: number;

  constructor(key: string, size: number, start: number) {
    this.key = key;
    this.size = size;
    this.start = start;
  }

  public remainSize(): number {
    return this.size - this.start;
  }

  public eat(consumedSize: number): void {
    this.start += consumedSize;
  }

  public clone(): S3File {
    return new S3File(this.key, this.size, this.start);
  }
}
