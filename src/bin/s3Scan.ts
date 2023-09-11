#!/usr/bin/env node

import { Command } from 'commander';
import { LIB_VERSION } from '../version';
import { S3Scan, S3_SCAN_HTTPS_AGENT } from '../lib';
import { S3ScanCLIOptions } from './s3ScanCliOptions';

const program = new Command();

program
  .version(LIB_VERSION)
  .description('A tool to list all keys in a bucket/prefix')
  .usage(`Usage: s3scan <s3url> [--concurrency=num] [--gunzip]`)
  .argument('<s3Url>', 'an S3 uri of the type s3://bucket/prefix')
  .option(
    '-g, --gunzip [boolean]',
    'boolean argument to unzip the data from stream',
    false,
  )
  .option('-c, --concurrency  [number]', 'Concurrency to use')
  .parse(process.argv);

const { concurrency, gunzip } = program.opts();
const options: S3ScanCLIOptions = {
  agent: S3_SCAN_HTTPS_AGENT,
  body: true,
  concurrency,
  gunzip,
};
const s3Url = program.args[0];
S3Scan.Scan(s3Url, options).pipe(process.stdout);
