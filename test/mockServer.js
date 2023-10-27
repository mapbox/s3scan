const http = require('http');
const AWS = require('aws-sdk');

// Set up dummy AWS credentials for testing
process.env.AWS_ACCESS_KEY_ID = 'dummy';
process.env.AWS_SECRET_ACCESS_KEY = 'dummy';
process.env.AWS_REGION = 'us-east-1';

module.exports = function mock() {
  const server = http.createServer(function (req, res) {
    s3.attempts++

    var routes = {
      timeout: /^\/timeout/,
      truncated: /^\/truncated/,
      listTruncated: /^\/\?prefix=list-truncated/,
      empty: /^\/\?prefix=empty/
    };
    if (routes.timeout.test(req.url)) {
      return setTimeout(function() {
        res.writeHead(200);
        res.end();
      }, 15);
    }

    if (routes.truncated.test(req.url)) {
      const responseText = 'This is not 100 characters.'
      res.writeHead(200, { 'Content-Length': responseText.length*2 });
      res.write(responseText);
      res.end()
    }

    if (routes.listTruncated.test(req.url)) {
      res.writeHead(200, { 'Content-Length': 500 });
      res.write('<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Name>bucket</Name><Prefix>list-truncated</Prefix><Marker>next-one</Marker><MaxKeys>1000</MaxKeys><IsTruncated>true</IsTruncated><Contents><Key>first-one</Key><LastModified>2015-09-24T21:09:21.000Z</LastModified><ETag>&quot;9a194fc78eaede');
      res.end();
      return req.socket.destroy();
    }

    if (routes.empty.test(req.url)) {
      res.writeHead(200);
      return res.end();
    }
    res.writeHead(404);
    res.end();
  });

  const s3 = {
    start: server.listen.bind(server, 3000),
    stop: server.close.bind(server),
    attempts: 0,
    client: new AWS.S3({
      endpoint: new AWS.Endpoint('http://localhost:3000'),
      s3BucketEndpoint: true,
      httpOptions: {
        timeout: 10
      }
    })
  };

  return s3;
}
