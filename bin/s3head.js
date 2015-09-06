#!/usr/bin/env node

var keepalive = require('agentkeepalive');
var agent = new keepalive.HttpsAgent({
  keepAlive: true,
  maxSockets: Math.ceil(require('os').cpus().length * 16),
  keepAliveTimeout: 60000
});
var s3scan = require('..');
var stream = require('stream');
var util = require('util');

var s3url = process.argv[2];
if (!s3url) {
  console.error('Usage: s3head <s3url>');
  process.exit(1);
}

var toString = new stream.Transform({ objectMode: true });
toString._transform = function(head, enc, callback) {
  callback(null, JSON.stringify(head));
};

s3scan.ScanHeaders(s3url, agent)
  .pipe(toString)
  .pipe(process.stdout);
