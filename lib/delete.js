var stream = require('stream');
var queue = require('queue-async');
var AWS = require('aws-sdk');
var s3 = new AWS.S3();
var awsError = require('./error.js');

module.exports = function(bucket, options) {
  if (typeof bucket === 'object') {
    options = bucket;
    bucket = null;
  }

  options = options || {};
  if (!bucket) options.objectMode = true;

  var deleteStream = new stream.Writable(options);
  deleteStream.pending = 0;
  deleteStream.deleted = 0;
  deleteStream.queue = queue();

  var finished = deleteStream.end.bind(deleteStream);
  deleteStream.end = function() {
    deleteStream.queue.awaitAll(function(err) {
      if (err) return deleteStream.emit('error', err);
      finished();
    });
  }

  deleteStream.queue.awaitAll(function(err) {
    if (err) return deleteStream.emit('error', err);
  });

  deleteStream._write = function(key, enc, callback) {
    if (deleteStream.pending > 1000)
      return setImmediate(deleteStream._write.bind(deleteStream), key, enc, callback);

    deleteStream.pending++;
    deleteStream.queue.defer(function(next) {
      var params = options.objectMode ? key : {
        Bucket: bucket,
        Key: key.toString()
      };

      s3.deleteObject(params, function(err, data) {
        deleteStream.pending--;
        if (err && err.statusCode !== 404) return next(awsError(err, params));
        if (err) console.log(awsError(err, params));
        deleteStream.deleted++;
        next();
      });
    });

    callback();
  };

  return deleteStream
};
