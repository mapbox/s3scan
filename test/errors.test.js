var tape = require('tape');
var http = require('http');
var AWS = require('aws-sdk');
var Get = require('../lib/get');
var Keys = require('../lib/keys');

function mock() {
  var server = http.createServer(function (req, res) {
    s3.attempts++

    var routes = {
      timeout: /^\/timeout/,
      truncated: /^\/truncated/,
      listTruncated: /^\/\?prefix=list-truncated/
    };

    if (routes.timeout.test(req.url)) {
      return setTimeout(function() {
        res.writeHead(200);
        res.end();
      }, 15);
    }

    if (routes.truncated.test(req.url)) {
      res.writeHead(200, { 'Content-Length': 100 });
      res.write('Not 100 characters');
      return req.socket.destroy();
    }

    if (routes.listTruncated.test(req.url)) {
      res.writeHead(200, { 'Content-Length': 500 });
      res.write('<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Name>bucket</Name><Prefix>list-truncated</Prefix><Marker>next-one</Marker><MaxKeys>1000</MaxKeys><IsTruncated>true</IsTruncated><Contents><Key>first-one</Key><LastModified>2015-09-24T21:09:21.000Z</LastModified><ETag>&quot;9a194fc78eaede');
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

test('[errors] truncated get', function(assert) {
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

test('[errors] truncated get, passErrors: true', function(assert) {
  var mock = this;
  var get = Get('-', { s3: mock.client, passErrors: true });
  get.on('data', function(err) {
    assert.equal(err.code, 'TruncatedResponseError', 'truncated error');
    assert.equal(mock.attempts, 4, 'tried 4 times');
    assert.end();
  });
  get.write('truncated/some/key');
  get.end();
});

test('[errors] truncated list response', function(assert) {
  var mock = this;
  var list = Keys('s3://bucket/list-truncated', { s3: mock.client });
  list.on('error', function(err) {
    assert.equal(err.code, 'XMLParserError', 'xml parsing error');
    assert.equal(mock.attempts, 4, 'tried 4 times');
    assert.end();
  });
  list.on('end', function() {
    assert.fail('should not complete successfully');
    assert.end();
  });
  list.resume();
});
