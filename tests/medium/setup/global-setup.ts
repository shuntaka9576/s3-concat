import {
  FlociContainer,
  type StartedFlociContainer,
} from '@floci/testcontainers';
import type { TestProject } from 'vitest/node';

export type TestS3Config =
  | {
      mode: 'floci';
      endpoint: string;
      region: string;
      accessKeyId: string;
      secretAccessKey: string;
    }
  | {
      mode: 'aws';
      region: string;
    };

declare module 'vitest' {
  export interface ProvidedContext {
    testS3Config: TestS3Config;
  }
}

let container: StartedFlociContainer | undefined;

export const setup = async ({ provide }: TestProject): Promise<void> => {
  if (process.env.USE_REAL_S3 === '1') {
    const region =
      process.env.AWS_REGION ?? process.env.AWS_DEFAULT_REGION ?? 'us-east-1';
    provide('testS3Config', { mode: 'aws', region });
    console.log(`using real AWS S3 (region=${region})`);
    return;
  }

  // Floci 2.2/2.3 require part checksums at completion but omit them from
  // UploadPartCopy responses. Pin the working image until that is fixed.
  container = await new FlociContainer(
    'floci/floci:2.0.1@sha256:4e451c39c7bb88e3cd4f87e8fc0c25d5b47695a51185d521e2241fa00486e8eb'
  ).start();
  provide('testS3Config', {
    mode: 'floci',
    endpoint: container.getEndpoint(),
    region: container.getRegion(),
    accessKeyId: container.getAccessKey(),
    secretAccessKey: container.getSecretKey(),
  });
  console.log('container launched');
};

export const teardown = async (): Promise<void> => {
  if (container !== undefined) {
    await container.stop();
    console.log('container stopped');
  }
};
