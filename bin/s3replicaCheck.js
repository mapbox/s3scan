#!/usr/bin/env node

var keepalive = require('agentkeepalive');
var agent = new keepalive.HttpsAgent({
  keepAlive: true,
  maxSockets: Math.ceil(require('os').cpus().length * 16),
  keepAliveTimeout: 60000
});
var s3scan = require('..');
var util = require('util');

var s3urls = process.argv.slice(2);
if (!s3urls.length === 2) {
  console.error('Usage: s3prefixed <primary s3url> <replica s3url>');
  process.exit(1);
}

s3scan.Compare(s3urls, agent)
  .on('error', function(err) { throw err; })
  .on('data', function(progress) {
    var msg = util.format(
      '\r\033[K%s primary scanned - %s replica scanned - %s discrepancies',
      progress.scanned[0], progress.scanned[1], progress.discrepancies
    );
    process.stdout.write(msg);
  })
  .on('end', function() {
    console.log('');
  });
