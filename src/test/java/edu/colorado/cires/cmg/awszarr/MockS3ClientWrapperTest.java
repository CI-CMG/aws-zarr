package edu.colorado.cires.cmg.awszarr;

import static edu.colorado.cires.cmg.awszarr.ZarrStoreBuilder.createTestGeoStore;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collection;
import java.util.TreeSet;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.commons.io.IOUtils;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;
import software.amazon.awssdk.services.s3.model.S3Object;

/**
 * This test class is a sanity check that the MockS3ClientWrapper behaves like the actual S3 implementation, otherwise all other tests would be
 * invalid.
 */
public class MockS3ClientWrapperTest {

  private static final Path MOCK_BUCKETS_DIR = Paths.get("target/mock-buckets");
  private static final String BUCKET = "my-test-bucket";
  private static final String ZARR_KEY = "test-zarr/geo-data.zarr";
  private static final Path BUCKET_DIR = MOCK_BUCKETS_DIR.resolve(BUCKET);

  private static final S3ClientWrapper s3ClientWrapper = FileMockS3ClientWrapper.builder().mockBucketDir(MOCK_BUCKETS_DIR).build();

  @BeforeAll
  public static void setup() throws Exception {
    createTestGeoStore(BUCKET_DIR, ZARR_KEY);
  }

  @Test
  public void testGetObject() throws Exception {

    String expected = new String(Files.readAllBytes(BUCKET_DIR.resolve(ZARR_KEY).resolve(".zattrs")), StandardCharsets.UTF_8);

    try (InputStream inputStream = s3ClientWrapper.getObject(BUCKET, ZARR_KEY + "/.zattrs").get()) {
      String actual = IOUtils.toString(inputStream, StandardCharsets.UTF_8);
      assertEquals(expected, actual);
    }

  }

  @Test
  public void testGetObjectMissing() throws Exception {
    assertFalse(s3ClientWrapper.getObject(BUCKET, ZARR_KEY + "/.foo").isPresent());
  }

  @Test
  public void testListObjectsV2Paginator() throws Exception {

    TreeSet<String> expected;
    try (Stream<Path> pathStream = Files.walk(BUCKET_DIR)) {
      expected = pathStream
          .filter(Files::isRegularFile)
          .map(path -> BUCKET_DIR.relativize(path))
          .map(Path::toString)
          .map(p -> p.replaceAll("\\\\", "/"))
          .collect(Collectors.toCollection(TreeSet::new));
    }

    TreeSet<String> keys;
    try(Stream<ListObjectsV2Response> stream = s3ClientWrapper.listObjectsV2Paginator(BUCKET, "")) {
      keys = stream
          .map(ListObjectsV2Response::contents)
          .flatMap(Collection::stream)
          .map(S3Object::key)
          .collect(Collectors.toCollection(TreeSet::new));
    }

    assertEquals(expected, keys);

    try(Stream<ListObjectsV2Response> stream = s3ClientWrapper.listObjectsV2Paginator(BUCKET, "test-zarr/geo-data.zarr/time")) {
      keys = stream
          .map(ListObjectsV2Response::contents)
          .flatMap(Collection::stream)
          .map(S3Object::key)
          .collect(Collectors.toCollection(TreeSet::new));
    }

    assertEquals(new TreeSet<>(Arrays.asList(
        "test-zarr/geo-data.zarr/time/.zarray",
        "test-zarr/geo-data.zarr/time/.zattrs",
        "test-zarr/geo-data.zarr/time/0",
        "test-zarr/geo-data.zarr/time/1"
    )), keys);

  }


}
