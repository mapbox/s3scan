import { Float } from 'aws-sdk/clients/batch'
import { int } from 'aws-sdk/clients/datapipeline'
import * as http from 'http'
import * as Stream from 'stream'

export interface S3ScanPurgeReturn {
  stream: Stream.Transform
  deletedCount: int
  rate: () => Float
}

export class S3Scan {
  public static Copy() {
    //Todo: Implentation Left
  }

  public static Delete() {
    //Todo: Implentation Left
  }

  public static Purge(
    s3Url: string,
    options: { [key: string]: string | boolean | http.Agent },
    callback: (err?: Error) => any,
  ): S3ScanPurgeReturn {
    //Todo: Implentation Left
    const readableStream = new Stream.Readable()
    const writableStream = new Stream.Transform()

    writableStream._write = (chunk, encoding, next) => {
      console.log(chunk.toString())
      console.log(encoding.toString())
      next()
    }

    readableStream.pipe(writableStream)

    readableStream.push(s3Url)
    readableStream.push(JSON.stringify(options))
    readableStream.push(null)

    callback()
    return {
      rate: () => 0,
      deletedCount: 0,
      stream: writableStream,
    }
  }

  public static Get() {
    //Todo: Implentation Left
  }

  public static List(
    s3Url: string,
    options: { [key: string]: http.Agent },
  ): Stream.Readable {
    //Todo: Implentation Left
    const readableStream = new Stream.Readable()
    readableStream.push(`s3URL: ${s3Url}`)
    readableStream.push(`options: ${JSON.stringify(options)}`)
    readableStream.push(null)
    return readableStream
  }

  public static Scan(
    s3Url: string,
    options: { [key: string]: string | boolean | http.Agent },
  ): Stream.Readable {
    //Todo: Implementation Left
    const readableStream = new Stream.Readable()
    readableStream.push(`s3URL: ${s3Url}`)
    readableStream.push(`options: ${JSON.stringify(options)}`)
    readableStream.push(null)
    return readableStream
  }
}
