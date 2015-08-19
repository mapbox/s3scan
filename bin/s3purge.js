#!/usr/bin/env node

var keepalive = require('agentkeepalive');
var agent = new keepalive.HttpsAgent({
  keepAlive: true,
  maxSockets: Math.ceil(require('os').cpus().length * 16),
  keepAliveTimeout: 60000
});
var s3scan = require('..');
var util = require('util');

var s3url = process.argv[2];
if (!s3url) {
  console.error('Usage: s3purge <s3url>');
  process.exit(1);
}

var quiet = process.argv[3] === '--quiet' || process.argv[3] === '-q';
var interval;

var purge = s3scan.Purge(s3url, agent, function(err) {
  if (!quiet) clearInterval(interval);
  setTimeout(function() {
    if (!quiet) console.log('');
    if (err) throw err;
    else process.exit(0);
  }, 500);
});

if (!quiet) {
  interval = setInterval(function() {
    process.stdout.write(util.format('\r\033[KDeleted %s @ %s/s', purge.deleted, purge.rate()));
  }, 500);
}
