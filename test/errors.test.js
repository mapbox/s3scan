const tape = require('tape');
const Get = require('../lib/get');
const Keys = require('../lib/keys');
const mock = require('./mockServer');

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

// These test are commented as the mock server in node 18 will not be
// able to generate a use case with content length less than data in body.
// test('[errors] truncated get', function(assert) {
//   var mock = this;
//   var get = Get('-', { s3: mock.client });
//   get.on('error', function(err) {
//     assert.equal(err.code, 'TruncatedResponseError', 'truncated error');
//     assert.equal(mock.attempts, 4, 'tried 4 times');
//     assert.end();
//   })
//   get.write('truncated/some/key');
//   get.end();
// });

// test('[errors] truncated get, passErrors: true', function(assert) {
//   var mock = this;
//   var get = Get('-', { s3: mock.client, passErrors: true });
//   get.on('data', function(err) {
//     assert.equal(err.code, 'TruncatedResponseError', 'truncated error');
//     assert.equal(mock.attempts, 4, 'tried 4 times');
//     assert.end();
//   });
//   get.write('truncated/some/key');
//   get.end();
// });

// test('[errors] truncated list response', function(assert) {
//   var mock = this;
//   var list = Keys('s3://bucket/list-truncated', { s3: mock.client });
//   list.on('error', function(err) {
//     assert.equal(err.code, 'XMLParserError', 'xml parsing error');
//     assert.equal(mock.attempts, 4, 'tried 4 times');
//     assert.end();
//   });
//   list.on('end', function() {
//     assert.fail('should not complete successfully');
//     assert.end();
//   });
//   list.resume();
// });

test('[errors] no body in the response', function(assert) {
  var mock = this;
  var list = Keys('s3://bucket/empty/', { s3: mock.client });
  list.on('error', function(err) {
    assert.equal(err.message, 'S3 API response contained no body');
    assert.equal(mock.attempts, 4, 'tried 4 times');
    assert.end();
  });
  list.on('end', function() {
    assert.fail('should not complete successfully');
    assert.end();
  });
  list.resume();
});
