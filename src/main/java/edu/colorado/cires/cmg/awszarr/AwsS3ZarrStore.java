package edu.colorado.cires.cmg.awszarr;

import com.bc.zarr.ZarrConstants;
import com.bc.zarr.storage.Store;
import edu.colorado.cires.cmg.s3out.S3OutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Objects;
import java.util.TreeSet;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import software.amazon.awssdk.services.s3.model.S3Object;

/**
 * An implementation of a {@link Store} that is backed by an AWS S3 bucket
 */
public class AwsS3ZarrStore implements Store {

  /**
   * Creates a new {@link Builder} that builds a new AwsS3ZarrStore
   *
   * @return a new {@link Builder} that builds a new AwsS3ZarrStore
   */
  public static Builder builder() {
    return new Builder();
  }

  /**
   * Builds a new {@link AwsS3ZarrStore}
   */
  public static class Builder {

    private String bucket;
    private String key;
    private S3ClientWrapper s3;
    private int multipartUploadMb = 5;
    private int maxUploadBuffers = 1;

    private Builder() {

    }

    /**
     * Sets the {@link S3ClientWrapper} representing S3 bucket actions
     * Required.
     *
     * @param s3 the {@link S3ClientWrapper}
     * @return this Builder
     */
    public Builder s3(S3ClientWrapper s3) {
      this.s3 = s3;
      return this;
    }

    /**
     * Sets the bucket name.
     * Required.
     *
     * @param bucket the bucket name
     * @return this Builder
     */
    public Builder bucket(String bucket) {
      this.bucket = bucket;
      return this;
    }

    /**
     * Sets the key prefix for a zarr store in a S3 bucket.
     * Required.
     *
     * @param key the key prefix for a zarr store in a S3 bucket
     * @return this Builder
     */
    public Builder key(String key) {
      this.key = key;
      return this;
    }

    /**
     * A {@link S3OutputStream} is used to upload files in parts. multipartUploadMb represents the size of the parts to
     * upload in MiB.  This value must be at least 5, which is the default.
     * Default: 5
     *
     * @param multipartUploadMb the part size in MiB
     * @return this Builder
     */
    public Builder multipartUploadMb(int multipartUploadMb) {
      this.multipartUploadMb = multipartUploadMb;
      return this;
    }

    /**
     * A {@link S3OutputStream} uses a queue to allow multipart uploads to S3 to happen while additional
     * buffers are being filled concurrently. The maxUploadBuffers defines the number of parts
     * to be queued before blocking population of additional parts.  The default value is 1.
     * Specifying a higher value may improve upload speed at the expense of more heap usage.
     * Using a value higher than one should be tested to see if any performance gains are achieved
     * for your situation.
     * Default: 1
     *
     * @param maxUploadBuffers the number of buffers in the queue before blocking
     * @return this Builder
     */
    public Builder maxUploadBuffers(int maxUploadBuffers) {
      this.maxUploadBuffers = maxUploadBuffers;
      return this;
    }

    /**
     * Builds a new {@link AwsS3ZarrStore}
     *
     * @return a new {@link AwsS3ZarrStore}
     */
    public AwsS3ZarrStore build() {
      return new AwsS3ZarrStore(s3, bucket, key, multipartUploadMb, maxUploadBuffers);
    }

  }

  private final String bucket;
  private final S3Path keyPrefix;
  private final S3ClientWrapper s3;
  private final int multipartUploadMb;
  private final int maxUploadBuffers;

  private AwsS3ZarrStore(S3ClientWrapper s3, String bucket, String keyPrefix, int multipartUploadMb, int maxUploadBuffers) {
    this.bucket = bucket.trim();
    this.keyPrefix = new S3Path(keyPrefix);
    this.s3 = Objects.requireNonNull(s3);
    this.multipartUploadMb = multipartUploadMb;
    this.maxUploadBuffers = maxUploadBuffers;
  }

  @Override
  public InputStream getInputStream(String key) throws IOException {
    return s3.getObject(bucket, keyPrefix.resolve(key).toString()).orElse(null);
  }

  @Override
  public OutputStream getOutputStream(String key) throws IOException {
    return S3OutputStream.builder()
        .s3(s3)
        .bucket(bucket)
        .key(keyPrefix.resolve(key).toString())
        .partSizeMib(multipartUploadMb)
        .uploadQueueSize(maxUploadBuffers)
        .build();
  }

  @Override
  public void delete(String key) throws IOException {
    s3.deleteObject(bucket, keyPrefix.resolve(key).toString());
  }

  private TreeSet<String> getParentsOf(String suffix) throws IOException {
    return getKeysEndingWith(suffix).stream()
        .map(S3Path::new)
        .map(S3Path::getParts)
        .map(parts -> new S3Path(parts.subList(0, parts.size() - 1)))
        .map(S3Path::toString)
        .collect(Collectors.toCollection(TreeSet::new));
  }

  @Override
  public TreeSet<String> getArrayKeys() throws IOException {
    return getParentsOf(ZarrConstants.FILENAME_DOT_ZARRAY);
  }

  @Override
  public TreeSet<String> getGroupKeys() throws IOException {
    return getParentsOf(ZarrConstants.FILENAME_DOT_ZGROUP);
  }

  @Override
  public TreeSet<String> getKeysEndingWith(String suffix) throws IOException {
    final S3Path suffixPath = new S3Path(suffix);
    try (Stream<S3Path> stream = getObjects(keyPrefix.toString())) {
      return stream
          .filter(path -> path.endsWith(suffixPath))
          .map(S3Path::getParts)
          .map(parts -> new S3Path(parts.subList(keyPrefix.size(), parts.size())))
          .map(S3Path::toString)
          .collect(Collectors.toCollection(TreeSet::new));
    }

  }

  private Stream<S3Path> getObjects(String prefix) {
    return s3.listObjectsV2Paginator(bucket, prefix)
        .flatMap(response -> response.contents().stream())
        .map(S3Object::key)
        .map(S3Path::new);
  }

  @Override
  public Stream<String> getRelativeLeafKeys(String key) throws IOException {
    final S3Path rootPath = keyPrefix.resolve(key);
    TreeSet<String> keys;
    try (Stream<S3Path> stream = getObjects(rootPath.toString())) {
      keys = stream
          .map(S3Path::getParts)
          .map(parts -> new S3Path(parts.subList(rootPath.size(), parts.size())))
          .filter(s3Path -> s3Path.size() > 0)
          .map(S3Path::toString)
          .collect(Collectors.toCollection(TreeSet::new));
    }
    return keys.stream();  // wrap in collection and then stream as caller does not close the stream
  }
}
