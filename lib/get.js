var stream = require('stream');
var queue = require('queue-async');
var AWS = require('aws-sdk');
var s3 = new AWS.S3();

module.exports = function(bucket, options) {
  var getStream = new stream.Transform(options);
  getStream.pending = 0;
  getStream.queue = queue();
  getStream.queue.awaitAll(function(err) { if (err) done(err); });

  getStream._transform = function(key, enc, callback) {
    if (getStream.pending > 1000)
      return setImmediate(getStream._transform.bind(getStream), key, enc, callback);

    getStream.pending++;
    getStream.queue.defer(function(next) {
      var params = {
        Bucket: bucket,
        Key: key.toString()
      };

      s3.getObject(params, function(err, data) {
        getStream.pending--;
        if (err && err.statusCode !== 404) return next(AwsError(err, params));
        if (err) log(AwsError(err, params));
        count++;
        getStream.push(data.Body + '\n');
        next();
      });
    });

    callback();
  };

  getStream._flush = function(callback) {
    getStream.queue.awaitAll(callback);
  };

};
