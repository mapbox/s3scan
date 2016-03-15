## `exports`

Returns a get transform stream. Converts S3 keys to objects retrieved
from a bucket via GET requests.

### Parameters

* `bucket` **`String`** 
* `options` **`Object`** 
  * `options.agent` **`Object`** An http agent to use for requests
  * `options.concurrency` **`Number`** Concurrency at which to request objects
  * `options.keys` **`Boolean`** Include RequestParameters on response objects
  * `options.gunzip` **`Boolean`** Gunzip each object body
  * `options.body` **`Boolean`** Stream only the object body from response objects
  * `options.passErrors` **`Boolean`** Include errors in object stream




## `s3scan.Copy`

Provides a writable stream that accepts keys and copies them to another location.

### Parameters

* `fromBucket` **`string`** the bucket to copy objects from
* `toBucket` **`string`** the bucket to copy objects into
* `keyTransform` **`[function]`** a function to transform keys. The function you provide should accept a source key and synchronously return the desired destination key. If not provided, objects in the `fromBucket` will be copied to the `toBucket` as-is.
* `options` **`[object]`** options to provide to the writable stream.



Returns `object` a writable stream

## `s3scan.Delete`

Provides a writable stream that expects you to write line-delimited S3 keys
into it, and performs an S3.deleteObject request on each key

### Parameters

* `bucket` **`string`** the S3 bucket from which to fetch keys
* `options` **`[object]`** options to provide to the writable stream
  * `options.agent` **`[object]`** an HTTPS agent to use for S3 requests
  * `options.s3` **`[object]`** an S3 client to use to make requests


### Examples

```js
require('s3scan').Delete('my-bucket')
  .write('some-key\n');
```

Returns `object` a writable stream

## `s3scan.Get`

Provides a transform stream that expects you to write line-delimited S3 keys
into it, and transforms them into a readable stream of S3.getObject responses

### Parameters

* `bucket` **`string`** the S3 bucket from which to fetch keys
* `options` **`[object]`** options to provide to the transform stream
  * `options.agent` **`[object]`** an HTTPS agent to use for S3 requests
  * `options.s3` **`[object]`** an S3 client to use to make requests
  * `options.passErrors` **`[boolean]`** if `true`, any error objects encountered will be passed into the readable stream
  * `options.keys` **`[boolean]`** if `true`, `.RequestParameters` (bucket and key) will be attached to the objects passed into the readable stream


### Examples

```js
require('s3scan').Get('my-bucket')
  .on('data', function(d) {
    console.log(JSON.stringify(d));
  })
  .write('some-key\n');
```

Returns `object` a transform stream

## `s3scan.List`

Provides a readable stream of keys beneath the provided S3 prefix

### Parameters

* `s3url` **`string`** an S3 uri of the type `s3://bucket/prefix`
* `options` **`[object]`** options to provide to the readable stream
  * `options.agent` **`[object]`** an HTTPS agent to use for S3 requests
  * `options.s3` **`[object]`** an S3 client to use to make requests


### Examples

```js
require('s3scan').List('s3://my-bucket/my-key')
  .pipe(process.stdout);
```

Returns `object` a readable stream of line-delimited keys

## `s3scan.Purge`

Deletes all objects beneath an S3 prefix

### Parameters

* `s3url` **`string`** an S3 uri of the type `s3://bucket/prefix`
* `options` **`[object]`** configuration options
  * `options.agent` **`[object]`** an HTTPS agent to use for S3 requests
  * `options.s3` **`[object]`** an S3 client to use to make requests
* `callback` **`[function]`** a function to run on error or on completion of deletes


### Examples

```js
require('s3scan').Purge('s3://my-bucket/my-key', function(err) {
  if (err) throw err;
  console.log('deleted all the things!');
});
```

Returns `object` a writable stream

## `s3scan.Scan`

Provides a readable stream of S3.getObject responses for all keys beneath the
provided S3 prefix

### Parameters

* `s3url` **`string`** an S3 uri of the type `s3://bucket/prefix`
* `options` **`[object]`** configuration options
  * `options.agent` **`[object]`** an HTTPS agent to use for S3 requests
  * `options.s3` **`[object]`** an S3 client to use to make requests
  * `options.passErrors` **`[boolean]`** if `true`, any error objects encountered will be passed into the readable stream
  * `options.keys` **`[boolean]`** if `true`, `.RequestParameters` (bucket and key) will be attached to the objects passed into the readable stream


### Examples

```js
require('s3scan').Scan('s3://my-bucket/my-key')
  .on('data', function(d) {
    console.log(JSON.stringify(d));
  });
```

Returns `object` a readable stream

