var AWS = require('aws-sdk');
var AwsError = require('./error');
var parallel = require('parallel-stream');

module.exports = function(fromBucket, toBucket, keyTransform, options) {
  options = options || {};

  if (typeof keyTransform === 'object') {
    options = keyTransform;
    keyTransform = function(key) { return key; };
  }

  var s3config = {
    maxRetries: 10,
    httpOptions: { connectTimeout: 3000 }
  };
  if (options.logger) s3config.logger = options.logger;
  if (options.agent) s3config.httpOptions.agent = options.agent;
  var s3 = options.s3 || new AWS.S3(s3config);

  function write(key, enc, callback) {
    key = key.toString().trim();
    if (!key.length) return callback();

    var copyParams = {
      Bucket: toBucket,
      Key: keyTransform(key.toString()),
      CopySource: [fromBucket, key].join('/')
    };

    s3.copyObject(copyParams, function(err) {
      if (err) return callback(AwsError(err, copyParams));
      copyStream.copied++;
      callback();
    });
  }

  var starttime = Date.now();
  var copyStream = parallel.writable(write, {
    objectMode: true,
    concurrency: options.concurrency
  });
  copyStream.copied = 0;

  copyStream.rate = function() {
    var duration = (Date.now() - starttime) / 1000;
    return (copyStream.copied / duration).toFixed(2);
  };

  return copyStream;
};
