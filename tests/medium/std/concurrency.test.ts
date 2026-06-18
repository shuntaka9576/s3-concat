import pLimit from '../../../lib/std/concurrency';

const flushMicrotasks = async (rounds = 5): Promise<void> => {
  for (let i = 0; i < rounds; i++) {
    await Promise.resolve();
  }
};

describe('pLimit', () => {
  test('does not exceed the configured concurrency', async () => {
    const limit = pLimit(2);
    let active = 0;
    let peak = 0;

    const releases: Array<() => void> = [];
    const tasks = Array.from({ length: 5 }, () =>
      limit(async () => {
        active++;
        if (active > peak) peak = active;
        await new Promise<void>((resolve) => releases.push(resolve));
        active--;
      })
    );

    await flushMicrotasks();
    expect(active).toBe(2);

    while (releases.length > 0) {
      releases.shift()?.();
      await flushMicrotasks();
    }
    await Promise.all(tasks);

    expect(peak).toBe(2);
  });

  test('starts queued tasks in FIFO order', async () => {
    const limit = pLimit(1);
    const startOrder: number[] = [];
    const releases: Array<() => void> = [];

    const tasks = [0, 1, 2, 3].map((i) =>
      limit(async () => {
        startOrder.push(i);
        await new Promise<void>((resolve) => releases.push(resolve));
      })
    );

    await flushMicrotasks();
    while (releases.length > 0) {
      releases.shift()?.();
      await flushMicrotasks();
    }
    await Promise.all(tasks);

    expect(startOrder).toEqual([0, 1, 2, 3]);
  });

  test('releases the slot when a task rejects so the next one starts', async () => {
    const limit = pLimit(1);
    const startOrder: number[] = [];

    const failing = limit(async () => {
      startOrder.push(0);
      throw new Error('boom');
    });
    const following = limit(async () => {
      startOrder.push(1);
    });

    await expect(failing).rejects.toThrow('boom');
    await following;

    expect(startOrder).toEqual([0, 1]);
  });

  test('treats a synchronous throw as a rejection without leaking the slot', async () => {
    const limit = pLimit(1);

    const sync = limit(() => {
      throw new Error('sync boom');
    });
    const next = limit(async () => 'ok');

    await expect(sync).rejects.toThrow('sync boom');
    await expect(next).resolves.toBe('ok');
  });

  test('runs tasks serially when concurrency is 1', async () => {
    const limit = pLimit(1);
    let active = 0;
    let peak = 0;

    await Promise.all(
      Array.from({ length: 4 }, () =>
        limit(async () => {
          active++;
          if (active > peak) peak = active;
          await Promise.resolve();
          active--;
        })
      )
    );

    expect(peak).toBe(1);
  });

  test.each([
    0,
    -1,
    1.5,
    Number.NaN,
  ])('throws TypeError for invalid concurrency %p', (value) => {
    expect(() => pLimit(value)).toThrow(TypeError);
  });
});
