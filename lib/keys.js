const { S3Client, ListObjectsCommand } = require('@aws-sdk/client-s3');
const stream = require('stream');
const s3urls = require('@mapbox/s3urls');
const awsError = require('./error.js');

module.exports = function(s3url, options) {
  options = options || {};

  const s3config = {
    maxAttempts: 10,
    requestTimeout: 3000
  };
  if (options.logger) s3config.logger = options.logger;
  if (options.agent) s3config.requestHandler = { httpsAgent: options.agent };
  const s3 = options.s3 || new S3Client(s3config);

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

    s3.send(new ListObjectsCommand(params))
      .then(function(data) {
        if (!Array.isArray(data.Contents)) {
          var error = new Error('Invalid SDK response');
          error.body = data.$metadata?.httpStatusCode;
          error.headers = data.$metadata?.httpHeaders;
          error.statusCode = data.$metadata?.httpStatusCode;
          error.requestId = data.$metadata?.requestId;
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
      })
      .catch(function(err) {
        keyStream.emit('error', awsError(err, params));
      });
  };

  return keyStream;
};
