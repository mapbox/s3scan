#!/usr/bin/env node

var https = require('https');

var agent = new https.Agent({
  keepAlive: true,
  maxSockets: Math.ceil(require('os').cpus().length * 16),
  keepAliveMsecs: 60000
});
var s3scan = require('..');
var argv = require('minimist')(process.argv);

var s3url = argv._[2];
if (!s3url) {
  console.error('Usage: s3scan <s3url> [--concurrency=num] [--gunzip]');
  process.exit(1);
}

var options = { agent: agent, body: true };
if (argv.gunzip) options.gunzip = new Boolean(argv.gunzip);
if (argv.concurrency) options.concurrency = parseInt(argv.concurrency, 10);

s3scan.Scan(s3url, options).pipe(process.stdout);
