import * as https from 'https';

export type S3ScanCLIOptions = {
  agent: https.Agent;
  dryrun?: boolean;
  quiet?: boolean;
  gunzip?: boolean;
  concurrency?: number;
  body?: boolean;
}
