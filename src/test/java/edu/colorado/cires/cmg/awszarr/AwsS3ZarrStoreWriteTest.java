package edu.colorado.cires.cmg.awszarr;

import static edu.colorado.cires.cmg.awszarr.ZarrStoreBuilder.createTestGeoStore;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.bc.zarr.storage.Store;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class AwsS3ZarrStoreWriteTest {

  private static final Path MOCK_BUCKETS_DIR = Paths.get("target/mock-buckets");
  private static final String BUCKET = "my-test-bucket";
  private static final Path BUCKET_DIR = MOCK_BUCKETS_DIR.resolve(BUCKET);

  @BeforeEach
  public void setup() throws Exception {
    FileUtils.deleteQuietly(BUCKET_DIR.toFile());
    Files.createDirectories(BUCKET_DIR);
  }

  @Test
  public void testGetOutputStream() throws Exception {
    String zarrKey = "foo/bar/test.zarr";
    createTestGeoStore(BUCKET_DIR, zarrKey);

    Store store = AwsS3ZarrStore.builder()
        .s3(FileMockS3ClientWrapper.builder().mockBucketDir(MOCK_BUCKETS_DIR).build())
        .bucket(BUCKET)
        .key(zarrKey)
        .build();

    String expected =
        "{\n"
            + "  \"groupId\" : 100\n"
            + "}";

    try (OutputStream outputStream = store.getOutputStream(".zattrs")) {
      IOUtils.write(expected, outputStream, StandardCharsets.UTF_8);
    }

    String actual = new String(Files.readAllBytes(BUCKET_DIR.resolve(zarrKey).resolve(".zattrs")), StandardCharsets.UTF_8);

    assertEquals(expected, actual);

    expected =
        "{\n"
            + "  \"groupId\" : 1001\n"
            + "}";

    try (OutputStream outputStream = store.getOutputStream("data/.zattrs")) {
      IOUtils.write(expected, outputStream, StandardCharsets.UTF_8);
    }

    actual = new String(Files.readAllBytes(BUCKET_DIR.resolve(zarrKey).resolve("data/.zattrs")), StandardCharsets.UTF_8);

    assertEquals(expected, actual);

  }

  @Test
  public void testDelete() throws Exception {
    String zarrKey = "foo/bar/test.zarr";
    createTestGeoStore(BUCKET_DIR, zarrKey);

    assertTrue(Files.exists(BUCKET_DIR.resolve(zarrKey).resolve(".zattrs")));
    assertTrue(Files.exists(BUCKET_DIR.resolve(zarrKey).resolve("data/.zattrs")));

    Store store = AwsS3ZarrStore.builder()
        .s3(FileMockS3ClientWrapper.builder().mockBucketDir(MOCK_BUCKETS_DIR).build())
        .bucket(BUCKET)
        .key(zarrKey)
        .build();

    store.delete(".zattrs");
    store.delete("data/.zattrs");

    assertFalse(Files.exists(BUCKET_DIR.resolve(zarrKey).resolve(".zattrs")));
    assertFalse(Files.exists(BUCKET_DIR.resolve(zarrKey).resolve("data/.zattrs")));
  }
}
