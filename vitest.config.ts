import { defineConfig, type ViteUserConfig } from 'vitest/config';

const config: ViteUserConfig = defineConfig({
  test: {
    name: 'medium',
    dir: 'tests/medium',
    globals: true,
    testTimeout: 100_000_000,
    coverage: {
      include: ['lib/**/*.ts'],
      exclude: ['lib/cli.ts', 'lib/cli/**'],
      reporter: ['json'],
    },
    globalSetup: 'tests/medium/setup/global-setup.ts',
  },
});

export default config;
