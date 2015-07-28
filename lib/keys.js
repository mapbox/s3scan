var AWS = require('aws-sdk');
var s3 = new AWS.S3();
var stream = require('stream');
var s3urls = require('s3urls');
var awsError = require('./error.js');

module.exports = function(s3url, options) {
  options = options || {};
  s3url = s3urls.fromUrl(s3url);

  var keyStream = new stream.Readable(options);
  keyStream.cache = [];
  keyStream.readPending = false;

  keyStream._read = function() {
    var keepReading = true;
    while (keyStream.cache.length && keepReading) {
      var key = keyStream.cache.shift();
      if (!key) continue;
      if (options.objectMode) key = { Bucket: s3url.Bucket, Key: key };
      keepReading = keyStream.push(key);
    }

    if (keyStream.cache.length) return;
    if (keyStream.readPending) return;
    if (keyStream.done) return keyStream.push(null);

    keyStream.readPending = true;

    var params = {
      Bucket: s3url.Bucket,
      Prefix: s3url.Key
    };

    if (keyStream.next) params.Marker = keyStream.next;

    s3.listObjects(params, function(err, data) {
      if (err) return keyStream.emit('error', awsError(err, params));

      var last = data.Contents.slice(-1)[0];
      var more = data.IsTruncated && last;

      data.Contents.forEach(function(item) {
          keyStream.cache.push(item.Key);
      });

      keyStream.readPending = false;

      if (more) keyStream.next = last.Key;
      else keyStream.done = true;
      keyStream._read();
    });
  };

  return keyStream;
};
