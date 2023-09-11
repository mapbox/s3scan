#!/usr/bin/env node

import { Command } from 'commander';
import { LIB_VERSION } from '../version';
import { S3Scan, S3_SCAN_HTTPS_AGENT } from '../lib';
import { S3ScanCLIOptions } from './s3ScanCliOptions';

const program = new Command();

program
  .version(LIB_VERSION)
  .description('A tool to list all keys in a bucket/prefix')
  .usage('Usage: s3scan <s3url> [--concurrency=num] [--gunzip]')
  .argument('<s3Url>', 'an S3 uri of the type s3://bucket/prefix')
  .parse(process.argv);

const options: S3ScanCLIOptions = { agent: S3_SCAN_HTTPS_AGENT };
const s3Url = program.args[0];
S3Scan.Keys(s3Url, options).pipe(process.stdout);
