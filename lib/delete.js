var stream = require('stream');
var queue = require('queue-async');
var AWS = require('aws-sdk');
var awsError = require('./error.js');

module.exports = function(bucket, options) {
  var s3config = {};
  if (options && options.agent) s3config.httpOptions = { agent: options.agent };
  var s3 = options && options.s3 || new AWS.S3(s3config);

  var starttime = Date.now();
  var deleteStream = new stream.Writable(options);
  deleteStream.pending = 0;
  deleteStream.deleted = 0;
  deleteStream.queue = queue();

  deleteStream.rate = function() {
    var duration = (Date.now() - starttime) / 1000;
    return (deleteStream.deleted / duration).toFixed(2);
  };

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
    key = key.toString();
    if (!key.length) return callback();

    if (deleteStream.pending > 1000)
      return setImmediate(deleteStream._write.bind(deleteStream), key, enc, callback);

    deleteStream.pending++;
    deleteStream.queue.defer(function(next) {
      var params = {
        Bucket: bucket,
        Key: key
      };

      s3.deleteObject(params, function(err, data) {
        deleteStream.pending--;
        if (err && err.statusCode !== 404) return next(awsError(err, params));
        deleteStream.deleted++;
        next();
      });
    });

    callback();
  };

  return deleteStream
};
