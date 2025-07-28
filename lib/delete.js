const { S3Client, DeleteObjectCommand } = require('@aws-sdk/client-s3');
const awsError = require('./error.js');
const parallel = require('parallel-stream');

module.exports = function(bucket, options) {
  options = options || {};

  const s3config = {
    maxAttempts: 10,
    requestTimeout: 3000
  };
  if (options.logger) s3config.logger = options.logger;
  if (options.agent) s3config.requestHandler = { httpsAgent: options.agent };
  const s3 = options.s3 || new S3Client(s3config);

  function write(key, enc, callback) {
    key = key.toString();
    if (!key.length) return callback();

    var params = {
      Bucket: bucket,
      Key: key
    };

    function removed(err) {
      if (err && err.name !== 'NoSuchKey' && err.name !== 'NoSuchBucket' && err.statusCode !== 404) return callback(awsError(err, params));
      deleteStream.deleted++;
      deleteStream.emit('deleted', key);
      callback();
    }

    if (options.dryrun) return removed();

    s3.send(new DeleteObjectCommand(params))
      .then(function() {
        removed();
      })
      .catch(function(err) {
        removed(err);
      });
  }

  var starttime = Date.now();
  var deleteStream = parallel.writable(write, {
    objectMode: true,
    concurrency: options.concurrency
  });
  deleteStream.deleted = 0;

  deleteStream.rate = function() {
    var duration = (Date.now() - starttime) / 1000;
    return (deleteStream.deleted / duration).toFixed(2);
  };

  return deleteStream
};
