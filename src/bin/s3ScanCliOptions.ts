import { int } from 'aws-sdk/clients/datapipeline';
import * as https from 'https';

// eslint-disable-next-line @typescript-eslint/consistent-type-definitions
export interface S3ScanCLIOptions {
  agent: https.Agent;
  dryrun?: boolean;
  quiet?: boolean;
  gunzip?: boolean;
  concurrency?: int;
  body?: boolean;
}
