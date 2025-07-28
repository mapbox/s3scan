const { S3Client, DeleteObjectCommand, S3ServiceException} = require('@aws-sdk/client-s3');

const { NodeHttpHandler } = require('@smithy/node-http-handler');
const { Agent } = require('http');
const awsError = require('./error.js');
const parallel = require('parallel-stream');

module.exports = function(bucket, options) {
  options = options || {};

  const s3config = {
    maxAttempts: 11, // Allows for 10 retries + 1 initial attempt
    requestTimeout: 3000,
    requestHandler: new NodeHttpHandler({
      httpAgent: new Agent({}),
      connectionTimeout: 20 * 1000,
      requestTimeout: 900 * 1000,
    }),
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
      if (err && err instanceof S3ServiceException && (
          err.name === 'NoSuchBucket' ||
              err.name === 'NoSuchKey' ||
              err.name === 'AccessDenied'
      )) return callback(awsError(err, params));
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
