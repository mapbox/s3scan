import { S3Scan } from '../../../src/lib'

describe('Testing S3Scan Class', () => {

  describe('Testing S3Scan validateS3URL', () => {

    it('Should not throw an error with a valid s3 URL', () => {
      const { bucket, key } =
        S3Scan.validateS3URL(`s3://mapbox/staging/orc/metrics_traces/dt=2018-07-18/`)
      expect(bucket).toEqual('mapbox')
      expect(key).toEqual('staging/orc/metrics_traces/dt=2018-07-18/')
    });

    it('Should throw an error with an incorrect S3 URI', () => {
      expect(S3Scan.validateS3URL(`s3:/&aksjdks`)).toThrow(`s3:/&aksjdks is not a valid s3 uri`)
    });

  });

});
