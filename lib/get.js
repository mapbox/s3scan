var stream = require('stream');
var queue = require('queue-async');
var AWS = require('aws-sdk');
var AwsError = require('./error');
var zlib = require('zlib');
var parallel = require('parallel-stream');

module.exports = function(bucket, options) {
  options = options || {};

  if (options.body && options.keys)
    throw new Error('options.body cannot be used with options.keys');

  if (options.body && options.passErrors)
    throw new Error('options.body cannot be used with options.passErrors');

  var s3config = {};
  if (options.agent) s3config.httpOptions = { agent: options.agent };
  var s3 = options.s3 || new AWS.S3(s3config);

  function transform(key, enc, callback) {
    key = key.toString();
    if (!key.length) return callback();

    var params = {
      Bucket: bucket,
      Key: key.toString()
    };

    s3.getObject(params, function(err, data) {
      if (!options.passErrors && err && err.statusCode === 404) return callback();
      if (!options.passErrors && err) return callback(AwsError(err, params));

      var response = err || data;
      if (options.keys) response.RequestParameters = params;

      if (options.gunzip) {
        zlib.gunzip(response.Body, function(err, gunzipped) {
          if (err) return getStream.emit('error', AwsError(err, params));
          response.Body = gunzipped;
          getStream.got++;
          callback(null, options.body ? response.Body : response);
        });
      } else {
        getStream.got++;
        callback(null, options.body ? response.Body : response);
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
  }

  var starttime = Date.now();
  var getStream = parallel.transform(transform, {
    objectMode: true,
    concurrency: options.concurrency
  });
  getStream.got = 0;

  getStream.rate = function() {
    var duration = (Date.now() - starttime) / 1000;
    return (getStream.got / duration).toFixed(2);
  };

  return getStream;
};
