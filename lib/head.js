var stream = require('stream');
var queue = require('queue-async');
var AWS = require('aws-sdk');
var AwsError = require('./error');

module.exports = function(bucket, options) {
  var s3config = {};
  if (options && options.agent) s3config.httpOptions = { agent: options.agent };
  var s3 = options && options.s3 || new AWS.S3(s3config);

  var starttime = Date.now();
  var headStream = new stream.Transform(options);
  headStream._readableState.objectMode = true;
  headStream.got = 0;
  headStream.pending = 0;
  headStream.queue = queue();
  headStream.queue.awaitAll(function(err) { if (err) done(err); });

  headStream.rate = function() {
    var duration = (Date.now() - starttime) / 1000;
    return (headStream.got / duration).toFixed(2);
  };

  headStream._transform = function(key, enc, callback) {
    key = key.toString();
    if (!key.length) return callback();

    if (headStream.pending > 1000)
      return setImmediate(headStream._transform.bind(headStream), key, enc, callback);

    headStream.pending++;
    headStream.queue.defer(function(next) {
      var params = {
        Bucket: bucket,
        Key: key.toString()
      };

      s3.headObject(params, function(err, data) {
        headStream.pending--;
        if (err && err.statusCode !== 404) return next(AwsError(err, params));
        if (err) return headStream.emit('error', AwsError(err, params));
        headStream.push(data);
        headStream.got++;
        next();
      });
    });

    callback();
  };

  headStream._flush = function(callback) {
    headStream.queue.awaitAll(callback);
  };

  return headStream;
};
