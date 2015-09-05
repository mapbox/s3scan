var stream = require('stream');
var Keys = require('./keys');
var s3urls = require('s3urls');

module.exports = function(s3url, options) {
  if (!/\{prefix\}/.test(s3url)) return Keys(s3url, options);
  options = options || {};

  s3url = s3urls.fromUrl(s3url);
  var combiner = new stream.PassThrough(options);
  combiner.setMaxListeners(Infinity);

  for (var i = 0; i < 256; i++) {
    var prefix = ('0' + i.toString(16)).slice(-2);
    var url = s3urls.toUrl(s3url.Bucket, s3url.Key.replace('{prefix}', prefix)).s3;
    connect(
      Keys(url, options)
        .on('end', finishedList)
        .on('error', function(err) { combiner.emit('error', err); })
    );
  }

  var running = 0;
  function connect(source) {
    if (running > 0) return setImmediate(connect, source);
    running++;
    source.pipe(combiner, { end: false });
  }

  var completed = 0;
  function finishedList() {
    completed++;
    running--;
    if (completed === 256) combiner.end();
  }

  return combiner;
};
