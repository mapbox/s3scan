var s3urls = require('@mapbox/s3urls');
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
 * @param {object} [options.s3] - an S3 client to use to make requests
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
 * @param {object} [options.s3] - an S3 client to use to make requests
 * @param {boolean} [options.passErrors] - if `true`, any error objects encountered
 * will be passed into the readable stream
 * @param {boolean} [options.keys] - if `true`, `.RequestParameters` (bucket and key)
 * will be attached to the objects passed into the readable stream
 * @param {boolean} [options.gunzip] - if `true`, gunzip each object body
 * @param {boolean} [options.body] - if `true` stream only the object body from response objects
 * @param {number} [options.concurrency] - concurrency at which to request objects
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
 * @param {object} [options.s3] - an S3 client to use to make requests
 * @param {number} [options.concurrency] - concurrency at which to delete objects
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
 * @param {object} [options] - configuration options
 * @param {object} [options.agent] - an HTTPS agent to use for S3 requests
 * @param {object} [options.s3] - an S3 client to use to make requests
 * @param {number} [options.concurrency] - concurrency at which to request objects
 * @param {boolean} [options.passErrors] - if `true`, any error objects encountered
 * will be passed into the readable stream
 * @param {boolean} [options.keys] - if `true`, `.RequestParameters` (bucket and key)
 * will be attached to the objects passed into the readable stream
 * @returns {object} a readable stream
 * @example
 * require('s3scan').Scan('s3://my-bucket/my-key')
 *   .on('data', function(d) {
 *     console.log(JSON.stringify(d));
 *   });
 */
module.exports.Scan = function(s3url, options) {
  var bucket = s3urls.fromUrl(s3url).Bucket;
  if (!bucket) throw new Error('Invalid s3url');

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
 * @param {object} [options] - configuration options
 * @param {object} [options.agent] - an HTTPS agent to use for S3 requests
 * @param {object} [options.s3] - an S3 client to use to make requests
 * @param {number} [options.concurrency] - concurrency at which to delete objects
 * @param {function} [callback] - a function to run on error or on completion of deletes
 * @returns {object} a writable stream
 * @example
 * require('s3scan').Purge('s3://my-bucket/my-key', function(err) {
 *   if (err) throw err;
 *   console.log('deleted all the things!');
 * });
 */
module.exports.Purge = function(s3url, options, callback) {
  if (typeof options === 'function') {
    callback = options;
    options = {};
  }

  var bucket = s3urls.fromUrl(s3url).Bucket;
  if (!bucket) throw new Error('Invalid s3url');

  var del = Delete(bucket, options)
    .on('error', callback || function() {})
    .on('finish', callback || function() {});
  var list = List(s3url, options)
    .on('error', callback || function() {});

  list.pipe(Split()).pipe(del);
  return del;
};

/**
 * Provides a writable stream that accepts keys and copies them to another location.
 *
 * @name s3scan.Copy
 * @param {string} fromBucket - the bucket to copy objects from
 * @param {string} toBucket - the bucket to copy objects into
 * @param {function} [keyTransform] - a function to transform keys. The
 * function you provide should accept a source key and synchronously return the
 * desired destination key. If not provided, objects in the `fromBucket` will be
 * copied to the `toBucket` as-is.
 * @param {object} [options] - options to provide to the writable stream.
 * @param {object} [options.agent] - an HTTPS agent to use for S3 requests
 * @param {object} [options.s3] - an S3 client to use to make requests
 * @param {number} [options.concurrency] - concurrency at which to copy objects
 * @returns {object} a writable stream
 */
module.exports.Copy = require('./lib/copy');
