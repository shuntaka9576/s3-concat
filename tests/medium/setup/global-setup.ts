import {
  FlociContainer,
  type StartedFlociContainer,
} from '@floci/testcontainers';
import type { GlobalSetupContext } from 'vitest/node';

export type FlociConfig = {
  endpoint: string;
  region: string;
  accessKeyId: string;
  secretAccessKey: string;
};

declare module 'vitest' {
  export interface ProvidedContext {
    flociConfig: FlociConfig;
  }
}

let container: StartedFlociContainer;

export const setup = async ({ provide }: GlobalSetupContext) => {
  container = await new FlociContainer().start();

  provide('flociConfig', {
    endpoint: container.getEndpoint(),
    region: container.getRegion(),
    accessKeyId: container.getAccessKey(),
    secretAccessKey: container.getSecretKey(),
  });

  console.log('container launched');
};

export const teardown = async () => {
  await container.stop();
  console.log('container stopped');
};
