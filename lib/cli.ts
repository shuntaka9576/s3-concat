#!/usr/bin/env node
import { run } from './cli/main';

run({ argv: process.argv.slice(2) }).then((code) => {
  process.exit(code);
});
