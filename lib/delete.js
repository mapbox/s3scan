var AWS = require('aws-sdk');
var awsError = require('./error.js');
var parallel = require('parallel-stream');

module.exports = function(bucket, options) {
  options = options || {};

  var s3config = {
    maxRetries: 10,
    httpOptions: { connectTimeout: 3000 }
  };
  if (options.logger) s3config.logger = options.logger;
  if (options.agent) s3config.httpOptions.agent = options.agent;
  var s3 = options.s3 || new AWS.S3(s3config);

  function write(key, enc, callback) {
    key = key.toString();
    if (!key.length) return callback();

    var params = {
      Bucket: bucket,
      Key: key
    };

    function removed(err) {
      if (err && err.statusCode !== 404) return callback(awsError(err, params));
      deleteStream.deleted++;
      deleteStream.emit('deleted', key);
      callback();
    }

    if (options.dryrun) return removed();
    s3.deleteObject(params, removed);
  };

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
