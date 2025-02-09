import { resolve } from 'node:path';
import { defineConfig } from 'vite';
import dts from 'vite-plugin-dts';

export default defineConfig({
  plugins: [
    dts({
      tsconfigPath: 'tsconfig.build.json',
    }),
  ],
  build: {
    rollupOptions: {
      external: ['@aws-sdk/client-s3', 'node:stream', 'node:stream/promises'],
    },
    lib: {
      entry: resolve(__dirname, './lib/s3-concat.ts'),
      name: 's3-concat',
      fileName: 's3-concat',
      formats: ['es', 'umd'],
    },
  },
});
