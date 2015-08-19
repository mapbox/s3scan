[![Build Status](https://travis-ci.org/mapbox/s3scan.svg?branch=master)](https://travis-ci.org/mapbox/s3scan)

# s3scan

Provides native Node.js streams for S3 operations such as
- listing all the keys in a bucket/prefix
- retrieving all the objects under a given bucket/prefix
- deleting all the children of a given bucket/prefix

Also provides CLI tools:

**s3keys**: prints a line-delimited list of keys to stdout

```sh
$ s3keys s3://my-bucket/some-prefix
```

**s3purge**: deletes all objects beneath the provided prefix

```sh
$ s3purge s3://my-bucket/all-finished-with-these
```

## Running tests

You can run tests against your own bucket/prefix by specifying them as environment variables:

```
TestBucket=my-bucket TestPrefix=my-prefix npm test
```

Otherwise these values default to buckets owned by Mapbox which require appropriate authentication.
