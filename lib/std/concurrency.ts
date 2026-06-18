export type LimitFunction = <T>(fn: () => Promise<T>) => Promise<T>;

const pLimit = (concurrency: number): LimitFunction => {
  if (!Number.isInteger(concurrency) || concurrency < 1) {
    throw new TypeError('concurrency must be a positive integer');
  }

  const queue: Array<() => void> = [];
  let active = 0;

  const next = (): void => {
    active--;
    const task = queue.shift();
    if (task !== undefined) task();
  };

  return <T>(fn: () => Promise<T>): Promise<T> =>
    new Promise<T>((resolve, reject) => {
      const run = (): void => {
        active++;
        // Wrap fn in Promise.resolve().then so a synchronous throw still
        // routes through reject and the finally(next) handler runs,
        // preventing active-count leaks that would stall the queue.
        Promise.resolve()
          .then(() => fn())
          .then(resolve, reject)
          .finally(next);
      };

      if (active < concurrency) {
        run();
      } else {
        queue.push(run);
      }
    });
};

export default pLimit;
