var stream = require('stream');
var queue = require('queue-async');
var AWS = require('aws-sdk');
var AwsError = require('./error');

module.exports = function(bucket, options) {
  var s3config = {};
  if (options && options.agent) s3config.httpOptions = { agent: options.agent };
  var s3 = options && options.s3 || new AWS.S3(s3config);

  var starttime = Date.now();
  var getStream = new stream.Transform(options);
  getStream._readableState.objectMode = true;
  getStream.got = 0;
  getStream.pending = 0;
  getStream.queue = queue();
  getStream.queue.awaitAll(function(err) { if (err) getStream.emit('error', err); });

  getStream.rate = function() {
    var duration = (Date.now() - starttime) / 1000;
    return (getStream.got / duration).toFixed(2);
  };

  getStream._transform = function(key, enc, callback) {
    key = key.toString();
    if (!key.length) return callback();

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
        if (err && err.statusCode === 404) return next();
        if (err) return getStream.emit('error', AwsError(err, params));
        getStream.push(data);
        getStream.got++;
        next();
      });
    });

    callback();
  };

  getStream._flush = function(callback) {
    getStream.queue.awaitAll(callback);
  };

  return getStream;
};
