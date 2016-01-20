var stream = require('stream');
var queue = require('queue-async');
var AWS = require('aws-sdk');
var AwsError = require('./error');

module.exports = function(fromBucket, toBucket, keyTransform, options) {
  if (typeof keyTransform === 'object') {
    options = keyTransform;
    keyTransform = function(key) { return key; };
  }

  var s3config = {};
  if (options && options.agent) s3config.httpOptions = { agent: options.agent };
  var s3 = options && options.s3 || new AWS.S3(s3config);

  var starttime = Date.now();
  var copyStream = new stream.Writable(options);
  copyStream.copied = 0;
  copyStream.pending = 0;
  copyStream.queue = queue();
  copyStream.queue.awaitAll(function(err) { if (err) copyStream.emit('error', err); });

  copyStream.rate = function() {
    var duration = (Date.now() - starttime) / 1000;
    return (copyStream.copied / duration).toFixed(2);
  };

  copyStream._write = function(key, enc, callback) {
    key = key.toString().trim();
    if (!key.length) return callback();

    if (copyStream.pending > 1000)
      return setImmediate(copyStream._write.bind(copyStream), key, enc, callback);

    copyStream.pending++;
    copyStream.queue.defer(function(next) {
      var getParams = {
        Bucket: fromBucket,
        Key: key.toString()
      };

      s3.getObject(getParams, function(err, data) {
        if (err) {
          copyStream.pending--;
          return next(AwsError(err, getParams));
        }

        var putParams = {
          Bucket: toBucket,
          Key: keyTransform(key.toString()),
          Body: data.Body
        };

        s3.putObject(putParams, function(err) {
          copyStream.pending--;
          if (err) return next(AwsError(err, putParams));
          copyStream.copied++;
          next();
        });
      });
    });

    callback();
  };

  var end = copyStream.end.bind(copyStream);
  copyStream.end = function() {
    if (!copyStream.pending) return end();

    copyStream.queue.awaitAll(function(err) {
      if (err) return copyStream.emit('error', err);
      end();
    });
  };

  return copyStream;
};
