import * as http from 'http';
import { S3Client } from '@aws-sdk/client-s3';

/***
 * Options to provide to the readable stream for S3ScanList
 */
export interface S3ScanOptions {
  /**
   * an HTTPS agent to use for S3 requests
   */
  agent: http.Agent;
  /**
   * an S3 client to use to make requests
   */
  s3: S3Client;
  /**
   * if `true`, any error objects encountered will be passed into the readable stream
   */
  passError?: boolean;
  /**
   * if `true`, `.RequestParameters` (bucket and key) will be attached to the objects passed into the readable stream
   */
  keys?: boolean;
  /**
   * if `true`, gunzip each object body
   */
  gunzip?: boolean;
  /**
   * if `true` stream only the object body from response objects
   */
  body?: boolean;
  /**
   * concurrency at which to request objects
   */
  concurrency?: number;
}
