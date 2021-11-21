package edu.colorado.cires.cmg.awszarr;

import edu.colorado.cires.cmg.s3out.S3ClientMultipartUpload;
import java.io.InputStream;
import java.util.Optional;
import java.util.stream.Stream;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;

/**
 * A wrapper around the S3Client from the AWS SDK v2.  This allows for calls to the S3Client to
 * be mocked for testing or allow for customization
 */
public interface S3ClientWrapper extends S3ClientMultipartUpload {

  /**
   * Creates a default instance of a S3ClientWrapper that works in most scenarios.
   *
   * @param s3 a {@link S3Client}
   * @return a new S3ClientWrapper
   */
  static S3ClientWrapper createDefault(S3Client s3) {
    return AwsS3ClientWrapper.builder().s3(s3).build();
  }

  /**
   * Returns an {@link Optional} that wraps an {@link InputStream} for reading a file from
   * a S3 bucket. An empty {@link Optional} will be returned if the file does not exist.
   *
   * @param bucket the bucket name
   * @param key a S3 key
   * @return an {@link Optional} that wraps an {@link InputStream} for reading a file from a S3 backed zarr store
   */
  Optional<InputStream> getObject(String bucket, String key);

  /**
   * Deletes a file from a S3 bucket.
   *
   * @param bucket the bucket name
   * @param key a S3 key
   */
  void deleteObject(String bucket, String key);

  /**
   * Returns a {@link Stream} representing the contents of a S3 bucket
   *
   * @param bucket the bucket name
   * @param prefix filters the results such that all objects start with this prefix
   * @return a {@link Stream} representing the contents of a S3 bucket
   */
  Stream<ListObjectsV2Response> listObjectsV2Paginator(String bucket, String prefix);

}
