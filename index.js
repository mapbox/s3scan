var s3urls = require('s3urls');
var Split = require('split');
var List = require('./lib/keys');
var Get = require('./lib/get');
var Head = require('./lib/head');
var Delete = require('./lib/delete');
var Prefixed = require('./lib/prefixed');
var stream = require('stream');

/**
 * Provides a readable stream of keys beneath the provided S3 prefix
 *
 * @name s3scan.List
 * @param {string} s3url - an S3 uri of the type `s3://bucket/prefix`
 * @param {object} [options] - options to provide to the readable stream
 * @param {object} [options.agent] - an HTTPS agent to use for S3 requests
 * @returns {object} a readable stream of line-delimited keys
 * @example
 * require('s3scan').List('s3://my-bucket/my-key')
 *   .pipe(process.stdout);
 */
module.exports.List = List;

/**
 * Provides a readable stream of keys beneath the provided S3 prefix, replacing
 * `{prefix}` in the input url with 0-255 hex characters [0-9a-f]
 *
 * @name s3scan.Prefixed
 * @param {string} s3url - an S3 uri of the type `s3://bucket/{prefix}/something`
 * @param {object} [options] - options to provide to the readable stream
 * @param {object} [options.agent] - an HTTPS agent to use for S3 requests
 * @returns {object} a readable stream of line-delimited keys
 * @example
 * require('s3scan').Prefixed('s3://my-bucket/{prefix}/my-key')
 *   .pipe(process.stdout);
 */
module.exports.Prefixed = Prefixed;

/**
 * Provides a transform stream that expects you to write line-delimited S3 keys
 * into it, and transforms them into a readable stream of S3.getObject responses
 *
 * @name s3scan.Get
 * @param {string} bucket - the S3 bucket from which to fetch keys
 * @param {object} [options] - options to provide to the transform stream
 * @param {object} [options.agent] - an HTTPS agent to use for S3 requests
 * @returns {object} a transform stream
 * @example
 * require('s3scan').Get('my-bucket')
 *   .on('data', function(d) {
 *     console.log(JSON.stringify(d));
 *   })
 *   .write('some-key\n');
 */
module.exports.Get = Get;

/**
 * Provides a transform stream that expects you to write line-delimited S3 keys
 * into it, and transforms them into a readable stream of S3.headObject responses
 *
 * @name s3scan.Head
 * @param {string} bucket - the S3 bucket from which to fetch keys
 * @param {object} [options] - options to provide to the transform stream
 * @param {object} [options.agent] - an HTTPS agent to use for S3 requests
 * @returns {object} a transform stream
 * @example
 * require('s3scan').Head('my-bucket')
 *   .on('data', function(d) {
 *     console.log(JSON.stringify(d));
 *   })
 *   .write('some-key\n');
 */
module.exports.Head = Head;

/**
 * Provides a writable stream that expects you to write line-delimited S3 keys
 * into it, and performs an S3.deleteObject request on each key
 *
 * @name s3scan.Delete
 * @param {string} bucket - the S3 bucket from which to fetch keys
 * @param {object} [options] - options to provide to the writable stream
 * @param {object} [options.agent] - an HTTPS agent to use for S3 requests
 * @returns {object} a writable stream
 * @example
 * require('s3scan').Delete('my-bucket')
 *   .write('some-key\n');
 */
module.exports.Delete = Delete;

/**
 * Provides a readable stream of S3.getObject responses for all keys beneath the
 * provided S3 prefix
 *
 * @name s3scan.Scan
 * @param {string} s3url - an S3 uri of the type `s3://bucket/prefix`
 * @param {object} [agent] - an HTTPS agent to use for S3 requests
 * @returns {object} a readable stream
 * @example
 * require('s3scan').Scan('s3://my-bucket/my-key')
 *   .on('data', function(d) {
 *     console.log(JSON.stringify(d));
 *   });
 */
module.exports.Scan = function(s3url, agent) {
  var bucket = s3urls.fromUrl(s3url).Bucket;
  if (!bucket) throw new Error('Invalid s3url');

  var options = agent ? { agent: agent } : undefined;

  var get = Get(bucket, options);
  var list = List(s3url, options)
    .on('error', function(err) {
      get.emit('error', err);
    });

  list.pipe(Split()).pipe(get);
  return get;
};

/**
 * Provides a readable stream of S3.headObject responses for all keys beneath the
 * provided S3 prefix
 *
 * @name s3scan.ScanHeaders
 * @param {string} s3url - an S3 uri of the type `s3://bucket/prefix`
 * @param {object} [agent] - an HTTPS agent to use for S3 requests
 * @returns {object} a readable stream
 * @example
 * require('s3scan').ScanHeaders('s3://my-bucket/my-key')
 *   .on('data', function(d) {
 *     console.log(JSON.stringify(d));
 *   });
 */
module.exports.ScanHeaders = function(s3url, agent) {
  var bucket = s3urls.fromUrl(s3url).Bucket;
  if (!bucket) throw new Error('Invalid s3url');

  var options = agent ? { agent: agent } : undefined;

  var get = Get(bucket, options);
  var list = List(s3url, options)
    .on('error', function(err) {
      get.emit('error', err);
    });

  list.pipe(Split()).pipe(get);
  return get;
};

/**
 * Deletes all objects beneath an S3 prefix
 *
 * @name s3scan.Purge
 * @param {string} s3url - an S3 uri of the type `s3://bucket/prefix`
 * @param {function} [callback] - a function to run on error or on completion of deletes
 * @param {object} [agent] - an HTTPS agent to use for S3 requests
 * @returns {object} a writable stream
 * @example
 * require('s3scan').Purge('s3://my-bucket/my-key', function(err) {
 *   if (err) throw err;
 *   console.log('deleted all the things!');
 * });
 */
module.exports.Purge = function(s3url, agent, callback) {
  if (typeof agent === 'function') {
    callback = agent;
    agent = null;
  }

  var bucket = s3urls.fromUrl(s3url).Bucket;
  if (!bucket) throw new Error('Invalid s3url');

  var options = agent ? { agent: agent } : undefined;

  function done(err) {
    if (callback) return callback(err);
    if (err) throw err;
  }

  var del = Delete(bucket, options)
    .on('error', callback || function() {})
    .on('finish', callback || function() {});
  var list = List(s3url, options)
    .on('error', callback || function() {});

  list.pipe(Split()).pipe(del);
  return del;
};

/**
 * Compare two S3 locations for identical looking keys
 *
 * @param
 * @returns
 */
module.exports.Compare = function(urls, agent) {
  var index = {};
  var scanned = [0, 0];
  var done = 0;

  var readable = new stream.Readable({ objectMode: true });

  readable._read = function() {
    if (readable.finished) return readable.push(null);

    setTimeout(function() {
      readable.push({
        scanned: scanned,
        discrepancies: Object.keys(index).length
      });
    }, 500);
  };

  readable.finishedScan = function() {
    done++;
    if (done === 2) readable.finished = true;
  };

  var add = new stream.Writable();
  add._write = function(key, enc, callback) {
    if (!key.length) return callback();
    key = key.toString();
    index[key] = index[key] || 0;
    index[key]++;
    if (index[key] === 2) delete index[key];
    scanned[0]++;
    callback();
  };

  Prefixed(urls[0], { agent: agent }).pipe(Split())
      .on('error', function(err) { readable.emit(err); })
    .pipe(add)
      .on('error', function(err) { readable.emit(err); })
      .on('finish', readable.finishedScan);

  Prefixed(urls[1], { agent: agent }).pipe(Split())
    .on('error', function(err) { readable.emit(err); })
    .on('end', readable.finishedScan)
    .on('data', function(key) {
      if (!key.length) return;
      key = key.toString();
      index[key] = index[key] || 0;
      index[key]++;
      if (index[key] === 2) delete index[key];
      scanned[1]++;
    });

  return readable;
};

/*
node bin/s3replicaCheck.js \
s3://tilestream-tilesets-production/{prefix}/willwhite/4h88h0k9/e22865434a38bf8830d201008032b197-3 \
s3://tilesets-eu-central-1/{prefix}/willwhite/4h88h0k9/e22865434a38bf8830d201008032b197-3

node bin/s3replicaCheck.js \
s3://tilestream-tilesets-production/{prefix}/arteku/cycleways_east/0945238b9b501ac5d65993978c87c179-385 \
s3://tilesets-eu-central-1/{prefix}/arteku/cycleways_east/0945238b9b501ac5d65993978c87c179-385

arteku/cycleways_east/0945238b9b501ac5d65993978c87c179-385
*/
