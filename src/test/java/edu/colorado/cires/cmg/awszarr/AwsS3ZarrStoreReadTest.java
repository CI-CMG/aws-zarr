package edu.colorado.cires.cmg.awszarr;

import static edu.colorado.cires.cmg.awszarr.ZarrStoreBuilder.createTestGeoStore;
import static org.junit.jupiter.api.Assertions.assertEquals;

import com.bc.zarr.ZarrArray;
import com.bc.zarr.ZarrGroup;
import com.bc.zarr.storage.Store;
import edu.colorado.cires.cmg.awszarr.ZarrStoreBuilder.TestData;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.commons.io.IOUtils;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

public class AwsS3ZarrStoreReadTest {

  private static final Path MOCK_BUCKETS_DIR = Paths.get("target/mock-buckets");
  private static final String BUCKET = "my-test-bucket";
  private static final Path BUCKET_DIR = MOCK_BUCKETS_DIR.resolve(BUCKET);

  @ParameterizedTest
  @ValueSource(strings = {"test-zarr/geo-data.zarr", "geo-data.zarr", ""})
  public void testGetInputStream(String zarrKey) throws Exception {
    createTestGeoStore(BUCKET_DIR, zarrKey);

    Store store = AwsS3ZarrStore.builder()
        .s3(FileMockS3ClientWrapper.builder().mockBucketDir(MOCK_BUCKETS_DIR).build())
        .bucket(BUCKET)
        .key(zarrKey)
        .build();

    String expected = new String(Files.readAllBytes(BUCKET_DIR.resolve(zarrKey).resolve(".zattrs")), StandardCharsets.UTF_8);

    try (InputStream inputStream = store.getInputStream(".zattrs")) {
      String actual = IOUtils.toString(inputStream, StandardCharsets.UTF_8);
      assertEquals(expected, actual);
    }
  }

  @ParameterizedTest
  @ValueSource(strings = {"test-zarr/geo-data.zarr", "geo-data.zarr", ""})
  public void testGetKeysEndingWith(String zarrKey) throws Exception {
    createTestGeoStore(BUCKET_DIR, zarrKey);

    Store store = AwsS3ZarrStore.builder()
        .s3(FileMockS3ClientWrapper.builder().mockBucketDir(MOCK_BUCKETS_DIR).build())
        .bucket(BUCKET)
        .key(zarrKey)
        .build();

    assertEquals(new HashSet<>(Arrays.asList(
            ".zgroup",
            "subGroup1/.zgroup",
            "subGroup1/subGroup11/.zgroup",
            "subGroup1/subGroup11/subGroup111/.zgroup",
            "subGroup2/.zgroup",
            "subGroup2/subGroup21/.zgroup"
        )),
        store.getKeysEndingWith(".zgroup")
    );

    assertEquals(
        Collections.singleton("subGroup1/subGroup11/subGroup11Array2/.zarray"),
        store.getKeysEndingWith("subGroup11Array2/.zarray")
    );
  }

  @ParameterizedTest
  @ValueSource(strings = {"test-zarr/geo-data.zarr", "geo-data.zarr", ""})
  public void testGetArrayKeys(String zarrKey) throws Exception {
    createTestGeoStore(BUCKET_DIR, zarrKey);

    Store store = AwsS3ZarrStore.builder()
        .s3(FileMockS3ClientWrapper.builder().mockBucketDir(MOCK_BUCKETS_DIR).build())
        .bucket(BUCKET)
        .key(zarrKey)
        .build();

    assertEquals(new HashSet<>(Arrays.asList(
            "data",
            "latitude",
            "longitude",
            "subGroup1/subGroup11/subGroup111/subGroup111Array1",
            "subGroup1/subGroup11/subGroup111/subGroup111Array2",
            "subGroup1/subGroup11/subGroup11Array1",
            "subGroup1/subGroup11/subGroup11Array2",
            "subGroup1/subGroup1Array1",
            "subGroup1/subGroup1Array2",
            "subGroup2/subGroup21/subGroup21Array1",
            "subGroup2/subGroup21/subGroup21Array2",
            "subGroup2/subGroup2Array1",
            "subGroup2/subGroup2Array2",
            "time"
        )),
        store.getArrayKeys()
    );
  }

  @ParameterizedTest
  @ValueSource(strings = {"test-zarr/geo-data.zarr", "geo-data.zarr", ""})
  public void testGetGroupKeys(String zarrKey) throws Exception {
    createTestGeoStore(BUCKET_DIR, zarrKey);

    Store store = AwsS3ZarrStore.builder()
        .s3(FileMockS3ClientWrapper.builder().mockBucketDir(MOCK_BUCKETS_DIR).build())
        .bucket(BUCKET)
        .key(zarrKey)
        .build();

    assertEquals(new HashSet<>(Arrays.asList(
            "",
            "subGroup1",
            "subGroup1/subGroup11",
            "subGroup1/subGroup11/subGroup111",
            "subGroup2",
            "subGroup2/subGroup21"
        )),
        store.getGroupKeys()
    );
  }

  @ParameterizedTest
  @ValueSource(strings = {"test-zarr/geo-data.zarr", "geo-data.zarr", ""})
  public void testGetRelativeLeafKeys(String zarrKey) throws Exception {
    createTestGeoStore(BUCKET_DIR, zarrKey);

    Store store = AwsS3ZarrStore.builder()
        .s3(FileMockS3ClientWrapper.builder().mockBucketDir(MOCK_BUCKETS_DIR).build())
        .bucket(BUCKET)
        .key(zarrKey)
        .build();
    Set<String> keys;
    try (Stream<String> stream = store.getRelativeLeafKeys("subGroup1/subGroup11")) {
      keys = stream.collect(Collectors.toSet());
    }

    assertEquals(new HashSet<>(Arrays.asList(
            "subGroup11Array2/.zarray",
            "subGroup111/subGroup111Array2/.zarray",
            "subGroup111/.zgroup",
            "subGroup111/subGroup111Array1/.zarray",
            ".zgroup",
            "subGroup11Array1/.zarray"
        )),
        keys
    );

  }

  @ParameterizedTest
  @ValueSource(strings = {"test-zarr/geo-data.zarr", "geo-data.zarr", ""})
  public void testReadUsage(String zarrKey) throws Exception {
    TestData expectedData = createTestGeoStore(BUCKET_DIR, zarrKey);

    Store store = AwsS3ZarrStore.builder()
        .s3(FileMockS3ClientWrapper.builder().mockBucketDir(MOCK_BUCKETS_DIR).build())
        .bucket(BUCKET)
        .key(zarrKey)
        .build();
    ZarrGroup root = ZarrGroup.open(store);
    ZarrArray longitude = root.openArray("longitude");
    ZarrArray latitude = root.openArray("latitude");
    ZarrArray time = root.openArray("time");
    ZarrArray data = root.openArray("data");

    TestData readData = new TestData(
        (long[]) longitude.read(),
        (long[]) latitude.read(),
        (long[]) time.read(),
        (int[]) data.read()
    );

    assertEquals(expectedData, readData);
    assertEquals(
        new HashMap<String, Object>() {{
          put("groupId", 0);
        }},
        root.getAttributes());
    assertEquals(
        new HashMap<String, Object>() {{
          put("arrayId", 0);
        }},
        longitude.getAttributes());
    assertEquals(
        new HashMap<String, Object>() {{
          put("arrayId", 1);
        }},
        latitude.getAttributes());
    assertEquals(
        new HashMap<String, Object>() {{
          put("arrayId", 2);
        }},
        time.getAttributes());
    assertEquals(
        new HashMap<String, Object>() {{
          put("arrayId", 3);
        }},
        data.getAttributes());

    assertEquals(new HashSet<>(Arrays.asList(
        "subGroup1",
        "subGroup1/subGroup11",
        "subGroup1/subGroup11/subGroup111",
        "subGroup2",
        "subGroup2/subGroup21"
    )), root.getGroupKeys());

    assertEquals(new HashSet<>(Arrays.asList(
        "data",
        "latitude",
        "longitude",
        "subGroup1/subGroup11/subGroup111/subGroup111Array1",
        "subGroup1/subGroup11/subGroup111/subGroup111Array2",
        "subGroup1/subGroup11/subGroup11Array1",
        "subGroup1/subGroup11/subGroup11Array2",
        "subGroup1/subGroup1Array1",
        "subGroup1/subGroup1Array2",
        "subGroup2/subGroup21/subGroup21Array1",
        "subGroup2/subGroup21/subGroup21Array2",
        "subGroup2/subGroup2Array1",
        "subGroup2/subGroup2Array2",
        "time"
    )), root.getArrayKeys());

  }


}