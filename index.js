var s3urls = require('s3urls');
var Split = require('split');
var List = require('./lib/keys');
var Get = require('./lib/get');
var Delete = require('./lib/delete');

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
module.exports.Purge = function(s3url, opts, callback) {
  if (typeof opts === 'function') {
    callback = opts;
    opts = {};
  }

  var bucket = s3urls.fromUrl(s3url).Bucket;
  if (!bucket) throw new Error('Invalid s3url');

  function done(err) {
    if (callback) return callback(err);
    if (err) throw err;
  }

  var del = Delete(bucket, opts)
    .on('error', callback || function() {})
    .on('finish', callback || function() {});
  var list = List(s3url, opts)
    .on('error', callback || function() {});

  list.pipe(Split()).pipe(del);
  return del;
};
