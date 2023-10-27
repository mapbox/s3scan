var https = require('https');
var agent = new https.Agent({
  keepAlive: true,
  maxSockets: Math.ceil(require('os').cpus().length * 16),
  keepAliveMsecs: 60000
});
var test = require('tape');
var crypto = require('crypto');
var d3 = require('d3-queue');
var s3urls = require('@mapbox/s3urls');
var _ = require('underscore');
var AWS = require('aws-sdk');
var s3 = new AWS.S3({ httpOptions: { agent: agent } });
var s3scan = require('../../');
var zlib = require('zlib');

var bucket = process.env.TestBucket || 'mapbox';
var prefix = process.env.TestPrefix || 's3scan-test';
var testId = crypto.randomBytes(16).toString('hex');
var uri = ['s3:/', bucket, prefix, testId].join('/');

console.log('\nTest uri: %s\n', uri);

var fixtures = {};
test('gzip fixtures', function(assert) {
  var queue = d3.queue();
  for (var i = 0; i < 2450; i++) {
    queue.defer(gzippify, crypto.randomBytes(16));
  }
  function gzippify(data, done) {
    zlib.gzip(data, function(err, gzdata) {
      if (err) return done(err);
      fixtures[data.toString('hex')] = gzdata;
      done();
    });
  }
  queue.awaitAll(function(err) {
    if (err) throw err;
    assert.end();
  });
});

test('load fixtures - please be patient...', function(assert) {
  var queue = d3.queue();

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

test('list keys (objectMode)', function(assert) {
  var found = [];
  var expected = Object.keys(fixtures).map(function(key) {
    return {
      Bucket: bucket,
      Key: [prefix, testId, key].join('/')
    };
  });

  var keys = Object.keys(fixtures).reduce(function(keys, key) {
    keys[[prefix, testId, key].join('/')] = true;
    return keys;
  }, {});

  s3scan.List(uri, { agent: agent, objectMode: true })
    .on('error', function(err) {
      assert.ifError(err, 'should not fail');
    })
    .on('data', function(key) {
      found.push(key);
    })
    .on('end', function() {
      assert.equal(found.length, expected.length, 'listed the right number of keys');
      assert.ok(found.every(function(item) {
        return item.Bucket === bucket && !!keys[item.Key];
      }), 'found all expected keys');
      assert.end();
    });
});

test('scan objects (option errors)', function(assert) {
  assert.throws(function() {
    s3scan.Scan(uri, { body: true, keys: true });
  }, /options\.body cannot be used with options\.keys/, 'throws with both options.body, options.keys');
  assert.throws(function() {
    s3scan.Scan(uri, { body: true, passErrors: true });
  }, /options\.body cannot be used with options\.passErrors/, 'throws with both options.body, options.passErrors');
  assert.end();
});

test('scan objects', function(assert) {
  var found = [];
  var objects = [];
  var expected = Object.keys(fixtures);

  s3scan.Scan(uri, { agent: agent, concurrency: 1000 })
    .on('error', function(err) { assert.ifError(err, 'should not fail'); })
    .on('data', function(data) { objects.push(data.Body); })
    .on('end', function() {
      var queue = d3.queue();
      for (var i = 0; i < objects.length; i++) {
        queue.defer(gunzip, objects[i]);
      }
      function gunzip(zbody, done) {
        zlib.gunzip(zbody, function(err, body) {
          if (err) return done(err);
          var key = body.toString('hex');
          if (!(key in fixtures)) assert.ok(key in fixtures, 'expected ' + key);
          found.push(key);
          done();
        });
      }
      queue.awaitAll(function(err) {
        assert.ifError(err);
        assert.equal(found.length, Object.keys(fixtures).length, 'retrieved all objects');
        assert.equal(_.difference(found, expected).length, 0, 'found all expected keys');
        assert.end();
      });
    });
});

test('scan objects, keys=true', function(assert) {
  var objects = [];

  s3scan.Scan(uri + '/0', { agent: agent, keys: true, concurrency: 1000 })
    .on('error', function(err) { assert.ifError(err, 'should not fail'); })
    .on('data', function(response) {
      objects.push(response.RequestParameters);
    })
    .on('end', function() {
      var found = objects.map(function(object) {
        return object.Key.split('/').pop();
      });
      var keys = Object.keys(fixtures).filter(function(k) {
        return k[0] === '0';
      });
      assert.equal(found.length, keys.length, 'retrieved all RequestParameters');
      assert.equal(_.difference(found, keys).length, 0, 'found all expected keys');
      assert.end();
    });
});

test('scan objects, concurrency=1', function(assert) {
  var found = [];
  var objects = [];

  s3scan.Scan(uri + '/0', { agent: agent, concurrency: 1 })
    .on('error', function(err) { assert.ifError(err, 'should not fail'); })
    .on('data', function(data) { objects.push(data.Body); })
    .on('end', function() {
      var queue = d3.queue();
      for (var i = 0; i < objects.length; i++) {
        queue.defer(gunzip, objects[i]);
      }
      function gunzip(zbody, done) {
        zlib.gunzip(zbody, function(err, body) {
          if (err) return done(err);
          var key = body.toString('hex');
          if (!(key in fixtures)) assert.ok(key in fixtures, 'expected ' + key);
          found.push(key);
          done();
        });
      }
      queue.awaitAll(function(err) {
        assert.ifError(err);
        var keys = Object.keys(fixtures);
        keys = keys.filter(function(k) { return k[0] === '0'; });
        assert.equal(found.length, keys.length, 'retrieved all objects');
        assert.deepEqual(found.sort(), keys.sort(), 'found all expected keys');
        assert.end();
      });
    });
});

test('scan objects, concurrency=1, gunzip', function(assert) {
  var objects = [];

  s3scan.Scan(uri + '/0', { agent: agent, concurrency: 1, gunzip: true })
    .on('error', function(err) { assert.ifError(err, 'should not fail'); })
    .on('data', function(data) { objects.push(data.Body.toString('hex')); })
    .on('end', function() {
      var keys = Object.keys(fixtures);
      keys = keys.filter(function(k) { return k[0] === '0'; });
      assert.equal(objects.length, keys.length, 'retrieved all objects');
      assert.deepEqual(objects.sort(), keys.sort(), 'found all expected keys');
      assert.end();
    });
});

test('scan objects, concurrency=1, gunzip, body', function(assert) {
  var objects = [];

  s3scan.Scan(uri + '/0', { agent: agent, concurrency: 1, gunzip: true, body: true })
    .on('error', function(err) { assert.ifError(err, 'should not fail'); })
    .on('data', function(data) { objects.push(data.toString('hex')); })
    .on('end', function() {
      var keys = Object.keys(fixtures);
      keys = keys.filter(function(k) { return k[0] === '0'; });
      assert.equal(objects.length, keys.length, 'retrieved all objects');
      assert.deepEqual(objects.sort(), keys.sort(), 'found all expected keys');
      assert.end();
    });
});

test('copying fixtures - please be patient', function(assert) {
  function keyTransform(key) {
    return key + '-1';
  }

  var cp = s3scan.Copy(bucket, bucket, keyTransform, { agent: agent, concurrency: 1000 });
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

  s3scan.Purge(uri, { agent: agent, concurrency: 1000 }, function(err) {
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
    .on('data', function() {
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
