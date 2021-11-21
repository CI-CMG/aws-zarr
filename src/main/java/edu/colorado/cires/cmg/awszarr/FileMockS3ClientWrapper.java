package edu.colorado.cires.cmg.awszarr;

import edu.colorado.cires.cmg.s3out.FileMockS3ClientMultipartUpload;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collection;
import java.util.Optional;
import java.util.TreeSet;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import software.amazon.awssdk.services.s3.model.CompletedPart;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;
import software.amazon.awssdk.services.s3.model.S3Object;

/**
 * A mock implementation of a {@link S3ClientWrapper} that is backed by the local filesystem. This should be used for testing ONLY.
 */
public class FileMockS3ClientWrapper implements S3ClientWrapper {

  /**
   * Creates a new {@link Builder} to build a FileMockS3ClientWrapper
   *
   * @return a new {@link Builder} to build a FileMockS3ClientWrapper
   */
  public static Builder builder() {
    return new Builder();
  }

  /**
   * Builds a new {@link FileMockS3ClientWrapper}
   */
  public static class Builder {

    private Path mockBucketDir;

    private Builder() {

    }

    /**
     * Sets the {@link Path} to a directory containing directories representing mock S3 buckets
     *
     * @param mockBucketDir a directory containing directories representing mock S3 buckets
     * @return this Builder
     */
    public Builder mockBucketDir(Path mockBucketDir) {
      this.mockBucketDir = mockBucketDir;
      return this;
    }

    /**
     * Builds a new {@link FileMockS3ClientWrapper}
     *
     * @return a new {@link FileMockS3ClientWrapper}
     */
    public FileMockS3ClientWrapper build() {
      return new FileMockS3ClientWrapper(mockBucketDir);
    }
  }

  private final Path mockBucketDir;
  private final FileMockS3ClientMultipartUpload s3Upload;

  private FileMockS3ClientWrapper(Path mockBucketDir) {
    this.mockBucketDir = mockBucketDir;
    this.s3Upload = FileMockS3ClientMultipartUpload.builder().mockBucketDir(mockBucketDir).build();
  }

  @Override
  public Optional<InputStream> getObject(String bucket, String key) {
    Path path = mockBucketDir.resolve(bucket).resolve(key);
    if (Files.isRegularFile(path)) {
      try {
        return Optional.of(Files.newInputStream(path));
      } catch (IOException e) {
        throw new IllegalStateException("Unable to open input stream: " + path, e);
      }
    }
    return Optional.empty();
  }

  @Override
  public void deleteObject(String bucket, String key) {
    Path path = mockBucketDir.resolve(bucket).resolve(key);
    if (Files.exists(path) && Files.isRegularFile(path)) {
      try {
        Files.delete(path);
      } catch (IOException e) {
        throw new IllegalStateException("Unable to delete file " + path, e);
      }
    }
  }

  @Override
  public Stream<ListObjectsV2Response> listObjectsV2Paginator(String bucket, String prefix) {
    Path bucketRoot = mockBucketDir.resolve(bucket);
    Path start = bucketRoot.resolve(prefix);
    TreeSet<String> results;
    try (Stream<Path> stream = Files.walk(start)) {
      results = stream
          .filter(Files::isRegularFile)
          .map(bucketRoot::relativize)
          .map(Path::normalize)
          .map(Path::toString)
          .map(key -> key.replaceAll("\\\\", "/"))
          .collect(Collectors.toCollection(TreeSet::new));
    } catch (IOException e) {
      throw new IllegalStateException("Unable to list files: " + start, e);
    }
    return results.stream().map(key -> ListObjectsV2Response.builder().contents(S3Object.builder().key(key).build()).build());
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
