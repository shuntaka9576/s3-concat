import { buildConcatFileNameCallback, parseCliArgs } from '../../lib/cli/parse';

describe('parseCliArgs', () => {
  const baseArgs = [
    '--src-bucket',
    'src',
    '--dst-bucket',
    'dst',
    '--src-prefix',
    'tmp/a',
    '--dst-prefix',
    'output',
    '--concat-file-name',
    'out.json',
  ];

  test('parses minimal valid args', () => {
    const got = parseCliArgs(baseArgs);
    if (got.kind !== 'run') throw new Error(`unexpected: ${got.kind}`);
    expect(got.config).toMatchObject({
      srcBucket: 'src',
      dstBucket: 'dst',
      srcPrefixes: ['tmp/a'],
      dstPrefix: 'output',
      output: { kind: 'name', name: 'out.json' },
      joinOrder: 'fetchOrder',
      dryRun: false,
      verbose: false,
      json: false,
    });
  });

  test('accepts repeated --src-prefix', () => {
    const got = parseCliArgs([
      ...baseArgs.filter(
        (_, i, a) => a[i - 1] !== '--src-prefix' && _ !== '--src-prefix'
      ),
      '--src-prefix',
      'a',
      '--src-prefix',
      'b',
    ]);
    if (got.kind !== 'run') throw new Error(`unexpected: ${got.kind}`);
    expect(got.config.srcPrefixes).toEqual(['a', 'b']);
  });

  test('parses --concat-file-name-template', () => {
    const got = parseCliArgs([
      '--src-bucket',
      's',
      '--dst-bucket',
      'd',
      '--src-prefix',
      'p',
      '--dst-prefix',
      'o',
      '--concat-file-name-template',
      'concat_{i}.json',
    ]);
    if (got.kind !== 'run') throw new Error(`unexpected: ${got.kind}`);
    expect(got.config.output).toEqual({
      kind: 'template',
      template: 'concat_{i}.json',
    });
  });

  test('parses --min-size, --p-limit, --join-order', () => {
    const got = parseCliArgs([
      ...baseArgs,
      '--min-size',
      '5GiB',
      '--p-limit',
      '20',
      '--join-order',
      'keyNameAsc',
    ]);
    if (got.kind !== 'run') throw new Error(`unexpected: ${got.kind}`);
    expect(got.config.minSize).toBe('5GiB');
    expect(got.config.pLimit).toBe(20);
    expect(got.config.joinOrder).toBe('keyNameAsc');
  });

  test('parses boolean flags', () => {
    const got = parseCliArgs([...baseArgs, '--dry-run', '--verbose', '--json']);
    if (got.kind !== 'run') throw new Error(`unexpected: ${got.kind}`);
    expect(got.config.dryRun).toBe(true);
    expect(got.config.verbose).toBe(true);
    expect(got.config.json).toBe(true);
  });

  test('--help short form', () => {
    expect(parseCliArgs(['-h']).kind).toBe('help');
    expect(parseCliArgs(['--help']).kind).toBe('help');
  });

  test('--version short form', () => {
    expect(parseCliArgs(['-v']).kind).toBe('version');
    expect(parseCliArgs(['--version']).kind).toBe('version');
  });

  test.each([
    ['--src-bucket'],
    ['--dst-bucket'],
    ['--src-prefix'],
    ['--dst-prefix'],
  ])('errors when %s is missing', (missing) => {
    const args = baseArgs.filter((tok, i, all) => {
      if (tok === missing) return false;
      if (i > 0 && all[i - 1] === missing) return false;
      return true;
    });
    const got = parseCliArgs(args);
    expect(got.kind).toBe('error');
    if (got.kind === 'error') {
      expect(got.message).toContain(missing);
    }
  });

  test('errors when both --concat-file-name and --concat-file-name-template are given', () => {
    const got = parseCliArgs([
      ...baseArgs,
      '--concat-file-name-template',
      'x_{i}.json',
    ]);
    expect(got.kind).toBe('error');
    if (got.kind === 'error') {
      expect(got.message).toMatch(/mutually exclusive/);
    }
  });

  test('errors when neither output name nor template is given', () => {
    const args = baseArgs.filter(
      (tok, i, all) =>
        tok !== '--concat-file-name' &&
        !(i > 0 && all[i - 1] === '--concat-file-name')
    );
    const got = parseCliArgs(args);
    expect(got.kind).toBe('error');
    if (got.kind === 'error') {
      expect(got.message).toMatch(/required/);
    }
  });

  test('errors when template has no {i}', () => {
    const got = parseCliArgs([
      '--src-bucket',
      's',
      '--dst-bucket',
      'd',
      '--src-prefix',
      'p',
      '--dst-prefix',
      'o',
      '--concat-file-name-template',
      'no_placeholder.json',
    ]);
    expect(got.kind).toBe('error');
    if (got.kind === 'error') {
      expect(got.message).toMatch(/\{i\}/);
    }
  });

  test('errors on invalid --join-order', () => {
    const got = parseCliArgs([...baseArgs, '--join-order', 'bogus']);
    expect(got.kind).toBe('error');
    if (got.kind === 'error') {
      expect(got.message).toMatch(/join-order/);
    }
  });

  test('errors on invalid --min-size', () => {
    const got = parseCliArgs([...baseArgs, '--min-size', '5xb']);
    expect(got.kind).toBe('error');
    if (got.kind === 'error') {
      expect(got.message).toMatch(/min-size/);
    }
  });

  test.each([
    '0',
    '-1',
    '1.5',
    'abc',
  ])('errors on invalid --p-limit value %s', (v) => {
    const got = parseCliArgs([...baseArgs, '--p-limit', v]);
    expect(got.kind).toBe('error');
    if (got.kind === 'error') {
      expect(got.message).toMatch(/p-limit/);
    }
  });

  test('errors on unknown option', () => {
    const got = parseCliArgs([...baseArgs, '--bogus']);
    expect(got.kind).toBe('error');
  });

  test('errors on positional argument', () => {
    const got = parseCliArgs([...baseArgs, 'positional']);
    expect(got.kind).toBe('error');
  });
});

describe('buildConcatFileNameCallback', () => {
  test('returns fixed name', () => {
    const cb = buildConcatFileNameCallback({ kind: 'name', name: 'out.json' });
    expect(cb()).toBe('out.json');
    expect(cb(7)).toBe('out.json');
  });

  test('substitutes {i} in template', () => {
    const cb = buildConcatFileNameCallback({
      kind: 'template',
      template: 'concat_{i}.json',
    });
    expect(cb(1)).toBe('concat_1.json');
    expect(cb(42)).toBe('concat_42.json');
  });

  test('defaults to index 1 when undefined', () => {
    const cb = buildConcatFileNameCallback({
      kind: 'template',
      template: 'x_{i}_{i}.txt',
    });
    expect(cb()).toBe('x_1_1.txt');
  });
});
