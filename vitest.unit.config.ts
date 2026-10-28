import { defineConfig, type ViteUserConfig } from 'vitest/config';

const config: ViteUserConfig = defineConfig({
  test: {
    name: 'unit',
    dir: 'tests/unit',
    globals: true,
    testTimeout: 10_000,
  },
});

export default config;
