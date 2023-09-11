#!/usr/bin/env node

import { Command } from 'commander';
import { LIB_VERSION } from '../version';
import { S3Scan, S3_SCAN_HTTPS_AGENT } from '../lib';
import { S3ScanCLIOptions } from './s3ScanCliOptions';

const program = new Command();

program
  .version(LIB_VERSION)
  .description(
    `A tool Provides a writable stream that expects you to write line-delimited S3 keys
    into it, and performs an S3.deleteObject request on each key`,
  )
  .usage('Usage: s3purge <s3url> [--concurrency=num] [--dryrun] [--quiet]')
  .argument('<s3Url>', 'an S3 uri of the type s3://bucket/prefix')
  .option('-d, --dryrun [boolean]', 'Simulate the deletion only', false)
  .option(
    '-q, --quiet  [boolean]',
    'Boolean value to run in silent mode',
    false,
  )
  .option('-c, --concurrency  [number]', 'Concurrency to use')
  .parse(process.argv);

const { concurrency, dryrun, quiet } = program.opts();
const options: S3ScanCLIOptions = {
  agent: S3_SCAN_HTTPS_AGENT,
  body: true,
  concurrency,
  dryrun,
  quiet,
};
const s3Url = program.args[0];

const interval: NodeJS.Timeout | undefined =
  !quiet && !dryrun
    ? setInterval(function () {
        process.stdout.write(
          `[KDeleted ${purge.deletedCount} @ ${purge.rate()}/s`,
        );
      }, 500)
    : undefined;

const purge = S3Scan.Purge(s3Url, options, err => {
  if (!quiet) clearInterval(interval);
  setTimeout(() => {
    if (!quiet) {
      console.log(purge.deletedCount);
    }
    if (err) throw err;
    else process.exit(0);
  }, 600);
});

purge.stream.pipe(process.stdout);

if (!quiet && !dryrun) {
  setInterval(function () {
    process.stdout.write(`[KDeleted ${purge.deletedCount} @ ${purge.rate()}/s`);
  }, 500);
}
