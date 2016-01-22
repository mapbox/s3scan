var keepalive = require('agentkeepalive');
var agent = new keepalive.HttpsAgent({
  keepAlive: true,
  maxSockets: Math.ceil(require('os').cpus().length * 16),
  keepAliveTimeout: 60000
});
var test = require('tape');
var crypto = require('crypto');
var async = require('queue-async');
var s3urls = require('s3urls');
var _ = require('underscore');
var AWS = require('aws-sdk');
var s3 = new AWS.S3({ httpOptions: { agent: agent } });
var s3scan = require('..');

var bucket = process.env.TestBucket || 'mapbox';
var prefix = process.env.TestPrefix || 's3scan-test';
var testId = crypto.randomBytes(16).toString('hex');
var uri = ['s3:/', bucket, prefix, testId].join('/');

console.log('\nTest uri: %s\n', uri);

var fixtures = {};
for (var i = 0; i < 2450; i++) {
  var data = crypto.randomBytes(16);
  fixtures[data.toString('hex')] = data;
}

test('load fixtures - please be patient...', function(assert) {
  var queue = async();

  Object.keys(fixtures).forEach(function(key) {
    var params = {
      Bucket: bucket,
      Key: [prefix, testId, key].join('/'),
      Body: fixtures[key]
    };

    queue.defer(s3.putObject.bind(s3), params);
  });

  queue.awaitAll(function(err) {
    if (err) throw err;
    assert.end();
  });
});

test('list keys', function(assert) {
  var found = [];
  var expected = Object.keys(fixtures).map(function(key) {
    return [prefix, testId, key].join('/');
  });

  s3scan.List(uri, { agent: agent })
    .on('error', function(err) {
      assert.ifError(err, 'should not fail');
    })
    .on('data', function(keys) {
      keys = keys.toString().trim();
      keys.split('\n').forEach(function(key) {
        found.push(key);
      });
    })
    .on('end', function() {
      assert.equal(found.length, expected.length, 'listed the right number of keys');
      assert.equal(_.difference(found, expected).length, 0, 'found all expected keys');
      assert.end();
    });
});

test('scan objects', function(assert) {
  var found = [];
  var expected = Object.keys(fixtures);

  s3scan.Scan(uri, { agent: agent })
    .on('error', function(err) {
      assert.ifError(err, 'should not fail');
    })
    .on('data', function(data) {
      var key = data.Body.toString('hex');
      assert.ok(key in fixtures, 'expected ' + key);
      found.push(key);
    })
    .on('end', function() {
      assert.equal(found.length, Object.keys(fixtures).length, 'retrieved all objects');
      assert.equal(_.difference(found, expected).length, 0, 'found all expected keys');
      assert.end();
    });
});

test('copying fixtures - please be patient', function(assert) {
  function keyTransform(key) {
    return key + '-1';
  }

  var cp = s3scan.Copy(bucket, bucket, keyTransform, { agent: agent });
  var ls = s3scan.List(uri, { agent: agent });
  var objects = 0;

  ls.pipe(cp)
    .on('error', function(err) { assert.ifError(err, 'should not error'); })
    .on('finish', function() {
      assert.equal(cp.copied, Object.keys(fixtures).length, 'copy stream reports number of copied objects');
      
      s3scan.List(uri, { agent: agent })
        .on('data', function(keys) {
          objects += keys.toString().trim().split('\n').length;
        })
        .on('end', function() {
          assert.equal(objects, 2 * Object.keys(fixtures).length, 'copied all objects');
          assert.end();
        });
    });
});

test('[dryrun] purging fixtures - please be patient...', function(assert) {
  var deletedEvents = 0;
  var remainingObjects = 0;

  s3scan.Purge(uri, { agent: agent, dryrun: true }, function(err) {
    assert.ifError(err, 'success');

    var params = s3urls.fromUrl(uri);

    s3scan.List(uri, { agent: agent })
      .on('data', function(keys) {
        remainingObjects += keys.toString().trim().split('\n').length;
      })
      .on('end', function() {
        assert.equal(deletedEvents, 2 * Object.keys(fixtures).length, 'all deleted events fired');
        assert.equal(remainingObjects, 2 * Object.keys(fixtures).length, 'no items removed');
        assert.end();
      });
  }).on('deleted', function() {
    deletedEvents++;
  });
});

test('purging fixtures - please be patient...', function(assert) {
  var deletedEvents = 0;

  s3scan.Purge(uri, agent, function(err) {
    assert.ifError(err, 'success');

    var params = s3urls.fromUrl(uri);

    s3.listObjects({
      Bucket: params.Bucket,
      Prefix: params.Key
    }, function(err, data) {
      if (err) throw err;
      assert.equal(deletedEvents, 2 * Object.keys(fixtures).length, 'all deleted events fired');
      assert.equal(data.Contents.length, 0, 'all items removed');
      assert.end();
    });
  }).on('deleted', function() {
    deletedEvents++;
  });
});

test('get stream no-op on 404', function(assert) {
  var getter = s3scan.Get(bucket, { agent: agent })
    .on('data', function(d) {
      assert.fail('no data should be transmitted');
    })
    .on('error', function(err) {
      assert.ifError(err, 'no error should arise');
    })
    .on('finish', function() { assert.end(); });

  getter.write([prefix, testId, 'does-not-exist'].join('/'));
  getter.end();
});

test('get stream allows errors to passthrough if configured', function(assert) {
  assert.plan(1);
  var getter = s3scan.Get(bucket, { agent: agent, passErrors: true })
    .on('data', function(d) {
      assert.equal(d.code, 'NoSuchKey', 'expected error object passed');
    })
    .on('error', function(err) {
      assert.ifError(err, 'no error should arise');
    });

  getter.write([prefix, testId, 'does-not-exist'].join('/'));
  getter.end();
});

test('get stream can provide request parameters', function(assert) {
  assert.plan(2);
  var getter = s3scan.Get(bucket, { agent: agent, passErrors: true, keys: true })
    .on('data', function(d) {
      assert.equal(d.code, 'NoSuchKey', 'expected error object passed');
      assert.deepEqual(d.RequestParameters, {
        Bucket: bucket,
        Key: [prefix, testId, 'does-not-exist'].join('/')
      }, 'expected parameters provided');
    })
    .on('error', function(err) {
      assert.ifError(err, 'no error should arise');
    });

  getter.write([prefix, testId, 'does-not-exist'].join('/'));
  getter.end();
});
