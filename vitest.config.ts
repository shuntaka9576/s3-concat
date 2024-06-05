import { defineConfig } from 'vitest/config';

export default defineConfig({
  test: {
    name: 'medium',
    dir: 'tests/medium',
    globals: true,
    testTimeout: 100_000_000,
    coverage: {
      include: ['lib/**/*.ts'],
      reporter: ['json'],
    },
    globalSetup: 'tests/medium/setup/global-setup.ts',
  },
});
