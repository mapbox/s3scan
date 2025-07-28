/**
 * Simple S3 URL parser to replace @mapbox/s3urls dependency
 * This avoids the transitive dependency on aws-sdk v2 through @mapbox/s3signed
 */

function fromUrl(s3url) {
  if (!s3url || typeof s3url !== 'string') {
    throw new Error('Invalid S3 URL');
  }

  // Remove s3:// prefix
  if (!s3url.startsWith('s3://')) {
    throw new Error('URL must start with s3://');
  }

  const urlWithoutProtocol = s3url.slice(5); // Remove 's3://'
  const firstSlashIndex = urlWithoutProtocol.indexOf('/');

  let bucket, key;

  if (firstSlashIndex === -1) {
    // No slash found, entire string is bucket name
    bucket = urlWithoutProtocol;
    key = '';
  } else {
    // Split at first slash
    bucket = urlWithoutProtocol.slice(0, firstSlashIndex);
    key = urlWithoutProtocol.slice(firstSlashIndex + 1);
  }

  return {
    Bucket: bucket,
    Key: key
  };
}

module.exports = {
  fromUrl: fromUrl
};
