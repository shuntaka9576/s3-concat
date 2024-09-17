import { resolve } from 'node:path';
import { readFile } from 'node:fs/promises';
import { defineConfig } from 'vite';
import dts from 'vite-plugin-dts';

const pkg = JSON.parse(
  await readFile(new URL('./package.json', import.meta.url), 'utf-8')
);

export default defineConfig({
  plugins: [
    dts({
      tsconfigPath: 'tsconfig.build.json',
    }),
  ],
  build: {
    rollupOptions: {
      external: Object.keys(pkg.dependencies),
    },
    lib: {
      entry: resolve(__dirname, './lib/s3-concat.ts'),
      name: 's3-concat',
      fileName: 's3-concat',
      formats: ['es', 'umd'],
    },
  },
});
