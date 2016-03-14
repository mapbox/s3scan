var stream = require('stream');
var queue = require('queue-async');
var AWS = require('aws-sdk');
var AwsError = require('./error');
var zlib = require('zlib');

/**
 * Returns a get transform stream. Converts S3 keys to objects retrieved
 * from a bucket via GET requests.
 * @param {String} bucket
 * @param {Object} options
 * @param {Object} options.agent An http agent to use for requests
 * @param {Number} options.concurrency Concurrency at which to request objects
 * @param {Boolean} options.gunzip Gunzip each object body
 * @param {Boolean} options.body Stream only the object body from response objects
 */
module.exports = function(bucket, options) {
  var s3config = {};
  if (options && options.agent) s3config.httpOptions = { agent: options.agent };
  var s3 = options && options.s3 || new AWS.S3(s3config);

  var concurrency = options && options.concurrency || undefined;
  var gunzip = options && options.gunzip || false;
  var body = options && options.body || false;

  var starttime = Date.now();
  var getStream = new stream.Transform(options);
  getStream._readableState.objectMode = true;
  getStream.got = 0;
  getStream.pending = 0;
  getStream.queue = queue(concurrency);
  getStream.queue.awaitAll(function(err) { if (err) getStream.emit('error', err); });

  getStream.rate = function() {
    var duration = (Date.now() - starttime) / 1000;
    return (getStream.got / duration).toFixed(2);
  };

  getStream._transform = function(key, enc, callback) {
    key = key.toString();
    if (!key.length) return callback();

    if (getStream.pending > 1000)
      return setImmediate(getStream._transform.bind(getStream), key, enc, callback);

    getStream.pending++;
    getStream.queue.defer(function(next) {
      var params = {
        Bucket: bucket,
        Key: key.toString()
      };

      s3.getObject(params, function(err, data) {
        getStream.pending--;
        if (!options.passErrors && err && err.statusCode === 404) return next();
        if (!options.passErrors && err) return getStream.emit('error', AwsError(err, params));

        var response = err || data;
        if (options.keys) response.RequestParameters = params;

        if (gunzip) {
          zlib.gunzip(response.Body, function(err, gunzipped) {
            if (err) return getStream.emit('error', AwsError(err, params));
            response.Body = gunzipped;
            getStream.push(body ? response.Body : response);
            getStream.got++;
            next();
          });
        } else {
          getStream.push(body ? response.Body : response);
          getStream.got++;
          next();
        }
      }).on('extractData', function(res) {
        if (res.data.Body.length !== Number(res.data.ContentLength)) {
          res.data = null;
          res.error = {
            code: 'TruncatedResponseError',
            message: 'Content-Length does not match response body length'
          };
        }
      }).on('retry', function(res) {
        if (res.error) {
          if (res.error.code === 'TruncatedResponseError') res.error.retryable = true;
        }
      });
    });

    callback();
  };

  getStream._flush = function(callback) {
    getStream.queue.awaitAll(callback);
  };

  return getStream;
};
