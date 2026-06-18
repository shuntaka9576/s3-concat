import { readFileSync } from 'node:fs';
import { defineConfig } from 'tsdown';

const pkg = JSON.parse(readFileSync('./package.json', 'utf8')) as {
  version: string;
};

export default defineConfig({
  entry: {
    's3-concat': 'lib/s3-concat.ts',
    cli: 'lib/cli.ts',
  },
  format: ['esm', 'cjs'],
  platform: 'node',
  target: 'node22',
  dts: true,
  minify: true,
  clean: true,
  outDir: 'dist',
  deps: { neverBundle: ['@aws-sdk/client-s3'] },
  define: {
    __S3_CONCAT_VERSION__: JSON.stringify(pkg.version),
  },
  outputOptions(opts) {
    const ext = opts.format === 'cjs' ? 'cjs' : 'mjs';
    return {
      ...opts,
      chunkFileNames: `[name]-[hash].${ext}`,
    };
  },
});
