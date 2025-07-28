const { S3Client, CopyObjectCommand } = require('@aws-sdk/client-s3');
const AwsError = require('./error');
const parallel = require('parallel-stream');

module.exports = function(fromBucket, toBucket, keyTransform, options) {
  options = options || {};

  if (typeof keyTransform === 'object') {
    options = keyTransform;
    keyTransform = function(key) { return key; };
  }

  var s3config = {
    maxAttempts: 10,
    requestTimeout: 3000
  };
  if (options.logger) s3config.logger = options.logger;
  if (options.agent) s3config.requestHandler = { httpsAgent: options.agent };
  var s3 = options.s3 || new S3Client(s3config);

  function write(key, enc, callback) {
    key = key.toString().trim();
    if (!key.length) return callback();

    var copyParams = {
      Bucket: toBucket,
      Key: keyTransform(key.toString()),
      CopySource: [fromBucket, key].join('/')
    };

    s3.send(new CopyObjectCommand(copyParams))
      .then(function() {
        copyStream.copied++;
        callback();
      })
      .catch(function(err) {
        callback(AwsError(err, copyParams));
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
