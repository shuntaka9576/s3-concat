import { Transform, type TransformCallback } from 'node:stream';

// S3 aws-chunked encoding rejects non-final chunks smaller than 8 KiB.
// 64 KiB matches the AWS SDK's default requestStreamBufferSize and gives
// enough headroom that GetObject's ~16 KiB IncomingMessage chunks always
// land above the threshold after one or two coalesce passes.
export const COALESCE_MIN = 64 * 1024;

export class CoalesceTransform extends Transform {
  private pending: Buffer[] = [];
  private pendingSize = 0;

  _transform(
    chunk: Buffer,
    _encoding: BufferEncoding,
    callback: TransformCallback
  ): void {
    this.pending.push(chunk);
    this.pendingSize += chunk.byteLength;

    if (this.pendingSize >= COALESCE_MIN) {
      const merged = Buffer.concat(this.pending, this.pendingSize);
      this.pending = [];
      this.pendingSize = 0;
      this.push(merged);
    }
    callback();
  }

  _flush(callback: TransformCallback): void {
    if (this.pendingSize > 0) {
      const merged = Buffer.concat(this.pending, this.pendingSize);
      this.pending = [];
      this.pendingSize = 0;
      this.push(merged);
    }
    callback();
  }
}
