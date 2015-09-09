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
var dryrun = process.argv[3] === '--dryrun' || process.argv[3] === '-d';
var interval;

var purge = s3scan.Purge(s3url, { agent: agent, dryrun: dryrun }, function(err) {
  if (!quiet) clearInterval(interval);
  setTimeout(function() {
    console.log(quiet ? purge.deleted : '');
    if (err) throw err;
    else process.exit(0);
  }, 600);
});

if (dryrun) {
  purge.on('deleted', function(key) {
    console.log('[dryrun] ' + key);
  });
}

if (!quiet && !dryrun) {
  interval = setInterval(function() {
    process.stdout.write(util.format('\r\033[KDeleted %s @ %s/s', purge.deleted, purge.rate()));
  }, 500);
}
