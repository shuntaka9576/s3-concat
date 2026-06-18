import { defineConfig } from 'vitest/config';

export default defineConfig({
  test: {
    name: 'unit',
    dir: 'tests/unit',
    globals: true,
    testTimeout: 10_000,
  },
});
