#!/usr/bin/env node

var concurrency = Math.ceil(require('os').cpus().length * 16);
require('https').globalAgent.maxSockets = concurrency;

var s3url = process.argv[2];

var keys = require('../lib/keys')(s3url, { objectMode: true });
var destroy = require('../lib/delete')();
var starttime = Date.now();

setInterval(function() {
  var duration = (destroy.deleted / ((Date.now() - starttime) / 1000)).toFixed(2);
  process.stdout.write('\r\033[K' + destroy.deleted + ' @ ' + duration + '/s');
}, 500).unref();

keys.pipe(destroy);
