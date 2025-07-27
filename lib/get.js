const { S3Client, GetObjectCommand } = require('@aws-sdk/client-s3');
const AwsError = require('./error');
const zlib = require('zlib');
const parallel = require('parallel-stream');

module.exports = function(bucket, options) {
  options = options || {};

  if (options.body && options.keys)
    throw new Error('options.body cannot be used with options.keys');

  if (options.body && options.passErrors)
    throw new Error('options.body cannot be used with options.passErrors');

  const s3config = {
    maxAttempts: 10,
    requestTimeout: 3000
  };
  if (options.logger) s3config.logger = options.logger;
  if (options.agent) s3config.requestHandler = { httpsAgent: options.agent };
  const s3 = options.s3 || new S3Client(s3config);

  function transform(key, enc, callback) {
    key = key.toString();
    if (!key.length) return callback();

    var params = {
      Bucket: bucket,
      Key: key.toString()
    };

    s3.send(new GetObjectCommand(params))
      .then(function(data) {
        var response = data;
        if (options.keys) response.RequestParameters = params;

        // Convert stream to buffer for compatibility
        if (data.Body && typeof data.Body.transformToByteArray === 'function') {
          data.Body.transformToByteArray()
            .then(function(bodyBytes) {
              response.Body = Buffer.from(bodyBytes);

              if (options.gunzip) {
                zlib.gunzip(response.Body, function(err, gunzipped) {
                  if (err) return getStream.emit('error', AwsError(err, params));
                  response.Body = gunzipped;
                  getStream.got++;
                  callback(null, options.body ? response.Body : response);
                });
              } else {
                getStream.got++;
                callback(null, options.body ? response.Body : response);
              }
            })
            .catch(function(err) {
              callback(AwsError(err, params));
            });
        } else {
          // Handle case where Body is already a buffer
          if (options.gunzip) {
            zlib.gunzip(response.Body, function(err, gunzipped) {
              if (err) return getStream.emit('error', AwsError(err, params));
              response.Body = gunzipped;
              getStream.got++;
              callback(null, options.body ? response.Body : response);
            });
          } else {
            getStream.got++;
            callback(null, options.body ? response.Body : response);
          }
        }
      })
      .catch(function(err) {
        if (!options.passErrors && err.name === 'NoSuchKey') return callback();
        if (!options.passErrors && err) return callback(AwsError(err, params));

        var response = err;
        if (options.keys) response.RequestParameters = params;
        getStream.got++;
        callback(null, options.body ? response.Body : response);
      });
  }

  var starttime = Date.now();
  var getStream = parallel.transform(transform, {
    objectMode: true,
    concurrency: options.concurrency
  });
  getStream.got = 0;

  getStream.rate = function() {
    var duration = (Date.now() - starttime) / 1000;
    return (getStream.got / duration).toFixed(2);
  };

  return getStream;
};
