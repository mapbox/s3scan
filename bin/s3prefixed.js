#!/usr/bin/env node

var keepalive = require('agentkeepalive');
var agent = new keepalive.HttpsAgent({
  keepAlive: true,
  maxSockets: Math.ceil(require('os').cpus().length * 16),
  keepAliveTimeout: 60000
});
var s3scan = require('..');

var s3url = process.argv[2];
if (!s3url) {
  console.error('Usage: s3prefixed <s3url>');
  process.exit(1);
}

s3scan.Prefixed(s3url, { agent: agent }).pipe(process.stdout);
