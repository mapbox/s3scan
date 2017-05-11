var AWS = require('aws-sdk');
var stream = require('stream');
var s3urls = require('s3urls');
var awsError = require('./error.js');
var assert = require('assert');

module.exports = function(s3url, options) {
  options = options || {};

  var s3config = {
    maxRetries: 10,
    httpOptions: { connectTimeout: 3000 }
  };
  if (options.agent) s3config.httpOptions.agent = options.agent;
  var s3 = options.s3 || new AWS.S3(s3config);

  s3url = s3urls.fromUrl(s3url);

  var starttime = Date.now();
  var keyStream = new stream.Readable(options);
  keyStream.listed = 0;
  keyStream.cache = [];
  keyStream.readPending = false;

  keyStream.rate = function() {
    var duration = (Date.now() - starttime) / 1000;
    return (keyStream.listed / duration).toFixed(2);
  };

  keyStream._read = function() {
    var keepReading = true;
    while (keyStream.cache.length && keepReading) {
      var key = keyStream.cache.shift();
      if (!key) continue;
      if (options.objectMode) key = { Bucket: s3url.Bucket, Key: key };
      else key = key + '\n';
      keepReading = keyStream.push(key);
      keyStream.listed++;
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

      if (!Array.isArray(data.Contents)) {
        var error = new Error('Invalid SDK response');
        error.body = this.httpResponse.body;
        error.headers = this.httpResponse.headers;
        error.statusCode = this.httpResponse.statusCode;
        error.requestId = this.requestId;
        return keyStream.emit('error', error);
      }

      var last = data.Contents.slice(-1)[0];
      var more = data.IsTruncated && last;

      data.Contents.forEach(function(item) {
        keyStream.cache.push(item.Key);
      });

      keyStream.readPending = false;

      if (more) keyStream.next = last.Key;
      else keyStream.done = true;
      keyStream._read();
    }).on('httpDone', function(response) {
      if (response.httpResponse.statusCode === 200 && !response.httpResponse.body.length) {
        response.error = new Error('S3 API response contained no body');
        response.error.retryable = true;
        response.error.requestId = this.requestId;
      }
    });
  };

  return keyStream;
};
