import {
  LocalstackContainer,
  type StartedLocalStackContainer,
} from '@testcontainers/localstack';
import ky from 'ky';
import type { GlobalSetupContext } from 'vitest/node';

declare module 'vitest' {
  export interface ProvidedContext {
    localStackHost: string;
  }
}

let container: StartedLocalStackContainer;

export const setup = async ({ provide }: GlobalSetupContext) => {
  container = await new LocalstackContainer().start();
  provide('localStackHost', container.getConnectionUri());

  // Wait until the LocalStack endpoint is available
  await waitForLocalStack(container.getConnectionUri());
  console.log('container lunched');
};

export const teardown = async () => {
  await container.stop();

  console.log('container stopped');
};

const waitForLocalStack = async (uri: string) => {
  const maxAttempts = 10;
  const timeout = 60 * 1000 * 5; // ms

  await ky.get(uri, {
    retry: {
      limit: maxAttempts,
    },
    timeout,
  });
};
