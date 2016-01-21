var tape = require('tape');
var http = require('http');
var AWS = require('aws-sdk');
var Get = require('../lib/get');

function mock() {
  var server = http.createServer(function (req, res) {
    s3.attempts++

    var routes = {
      timeout: /^\/timeout/,
      truncated: /^\/truncated/
    };

    if (routes.timeout.test(req.url)) {
      return setTimeout(function() {
        res.writeHead(200);
        res.end();
      }, 15);
    }

    if (routes.truncated.test(req.url)) {
      res.writeHead(200, { 'Content-Length': 100 });
      res.end('Not 100 characters');
      return req.socket.destroy();
    }

    res.writeHead(404);
    res.end();
  });

  var s3 = {
    start: server.listen.bind(server, 20009),
    stop: server.close.bind(server),
    attempts: 0,
    client: new AWS.S3({
      endpoint: new AWS.Endpoint('http://localhost:20009'),
      s3BucketEndpoint: true,
      httpOptions: { timeout: 10 }
    })
  };

  return s3;
}

function test(name, assertions) {
  tape(name, function(assert) {
    var server = mock();
    server.start(function(err) {
      if (err) throw err;

      var end = assert.end.bind(assert);
      assert.end = function(err) {
        server.stop(function(cl) {
          if (cl) throw cl;
          end(err);
        });
      };

      assertions.call(server, assert);
    });
  });
}

test('[errors] timeout', function(assert) {
  var mock = this;
  var get = Get('-', { s3: mock.client });
  get.on('error', function(err) {
    assert.equal(err.name, 'TimeoutError', 'timed out');
    assert.equal(mock.attempts, 4, 'tried 4 times');
    assert.end();
  })
  get.write('timeout/some/key');
  get.end();
});

test('[errors] truncated', function(assert) {
  var mock = this;
  var get = Get('-', { s3: mock.client });
  get.on('error', function(err) {
    assert.equal(err.code, 'TruncatedResponseError', 'truncated error');
    assert.equal(mock.attempts, 4, 'tried 4 times');
    assert.end();
  })
  get.write('truncated/some/key');
  get.end();
});
