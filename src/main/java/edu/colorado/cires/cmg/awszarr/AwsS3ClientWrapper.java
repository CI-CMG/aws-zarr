package edu.colorado.cires.cmg.awszarr;

import edu.colorado.cires.cmg.s3out.AwsS3ClientMultipartUpload;
import edu.colorado.cires.cmg.s3out.NoContentTypeResolver;
import edu.colorado.cires.cmg.s3out.S3ClientMultipartUpload;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Optional;
import java.util.stream.Stream;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.CompletedPart;
import software.amazon.awssdk.services.s3.model.DeleteObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;
import software.amazon.awssdk.services.s3.model.NoSuchKeyException;

/**
 * A {@link S3ClientWrapper} that uses a {@link S3Client} to access files from an S3 bucket.
 */
public class AwsS3ClientWrapper implements S3ClientWrapper {

  /**
   * Creates a new {@link Builder} to build a AwsS3ClientWrapper
   *
   * @return a new {@link Builder} to build a AwsS3ClientWrapper
   */
  public static Builder builder() {
    return new Builder();
  }

  /**
   * Builds a new {@link AwsS3ClientWrapper}.
   */
  public static class Builder {

    private S3Client s3;

    private Builder() {

    }

    /**
     * Sets the {@link S3Client}
     *
     * @param s3 the {@link S3Client}
     * @return this Builder
     */
    public Builder s3(S3Client s3) {
      this.s3 = s3;
      return this;
    }

    /**
     * Builds a new {@link AwsS3ClientWrapper}
     *
     * @return a new {@link AwsS3ClientWrapper}
     */
    public AwsS3ClientWrapper build() {
      return new AwsS3ClientWrapper(
          s3,
          AwsS3ClientMultipartUpload.builder().s3(s3).contentTypeResolver(new NoContentTypeResolver()).build());
    }

  }

  private final S3Client s3;
  private final S3ClientMultipartUpload s3Upload;

  private AwsS3ClientWrapper(S3Client s3, S3ClientMultipartUpload s3Upload) {
    this.s3 = s3;
    this.s3Upload = s3Upload;
  }

  @Override
  public Optional<InputStream> getObject(String bucket, String key) {
    try {
      return Optional.of(s3.getObject(GetObjectRequest.builder().bucket(bucket).key(key).build()));
    } catch (NoSuchKeyException e) {
      return Optional.empty();
    }
  }

  @Override
  public void deleteObject(String bucket, String key) {
    s3.deleteObject(DeleteObjectRequest.builder().bucket(bucket).key(key).build());
  }

  @Override
  public Stream<ListObjectsV2Response> listObjectsV2Paginator(String bucket, String prefix) {
    return s3.listObjectsV2Paginator(ListObjectsV2Request.builder().bucket(bucket).prefix(prefix).build()).stream();
  }

  @Override
  public String createMultipartUpload(String bucket, String key) {
    return s3Upload.createMultipartUpload(bucket, key);
  }

  @Override
  public CompletedPart uploadPart(String bucket, String key, String uploadId, int partNumber, ByteBuffer buffer) {
    return s3Upload.uploadPart(bucket, key, uploadId, partNumber, buffer);
  }

  @Override
  public void completeMultipartUpload(String bucket, String key, String uploadId, Collection<CompletedPart> completedParts) {
    s3Upload.completeMultipartUpload(bucket, key, uploadId, completedParts);
  }

  @Override
  public void abortMultipartUpload(String bucket, String key, String uploadId) {
    s3Upload.abortMultipartUpload(bucket, key, uploadId);
  }
}
