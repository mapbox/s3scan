import * as Stream from 'stream';
import { S3ScanCLIOptions } from '../bin';
import { S3Client, S3ClientConfig } from '@aws-sdk/client-s3';
import { Logger } from 'tslog';
const AmazonS3URI = require('amazon-s3-uri');

export interface S3ScanPurgeReturn {
  stream: Stream.Transform;
  deletedCount: number;
  rate: () => number;
}

export class S3Scan {
  static logger = new Logger();

  static getS3Client() {
    const s3_config: S3ClientConfig = {
      maxAttempts: 10,
      logger: this.logger,
    };
    return new S3Client(s3_config);
  }

  public static validateS3URL(s3URL: string) {
      return AmazonS3URI(s3URL);
  }

  public static Copy() {
    //Todo: Implentation Left
  }

  public static Delete() {
    //Todo: Implentation Left
  }

  public static Get() {
    //Todo: Implentation Left
  }

  public static Keys(
    s3Url: string,
    options: S3ScanCLIOptions,
  ): Stream.Readable {
    //Todo: Implentation Left
    const readableStream = new Stream.Readable();
    readableStream.push(`s3URL: ${s3Url}`);
    readableStream.push(`options: ${JSON.stringify(options)}`);
    readableStream.push(null);
    return readableStream;
  }

  public static Scan(
    s3Url: string,
    options: S3ScanCLIOptions,
  ): Stream.Readable {
    //Todo: Implementation Left
    const readableStream = new Stream.Readable();
    readableStream.push(`s3URL: ${s3Url}`);
    readableStream.push(`options: ${JSON.stringify(options)}`);
    readableStream.push(null);
    return readableStream;
  }

  public static Purge(
    s3Url: string,
    options: S3ScanCLIOptions,
    callback: (err?: Error) => any,
  ): S3ScanPurgeReturn {
    //Todo: Implentation Left
    const readableStream = new Stream.Readable();
    const writableStream = new Stream.Transform();

    writableStream._write = (chunk, encoding, next) => {
      console.log(chunk.toString());
      console.log(encoding.toString());
      next();
    };

    readableStream.pipe(writableStream);

    readableStream.push(s3Url);
    readableStream.push(JSON.stringify(options));
    readableStream.push(null);

    callback();
    return {
      rate: () => 0,
      deletedCount: 0,
      stream: writableStream,
    };
  }
}
