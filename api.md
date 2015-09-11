## `s3scan.Delete`

Provides a writable stream that expects you to write line-delimited S3 keys
into it, and performs an S3.deleteObject request on each key

### Parameters

* `bucket` **`string`** the S3 bucket from which to fetch keys
* `options` **`[object]`** options to provide to the writable stream


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


### Examples

```js
require('s3scan').Scan('s3://my-bucket/my-key')
  .on('data', function(d) {
    console.log(JSON.stringify(d));
  });
```

Returns `object` a readable stream


