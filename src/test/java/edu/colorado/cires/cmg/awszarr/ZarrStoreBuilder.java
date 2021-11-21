package edu.colorado.cires.cmg.awszarr;

import com.bc.zarr.ArrayParams;
import com.bc.zarr.Compressor;
import com.bc.zarr.CompressorFactory;
import com.bc.zarr.DataType;
import com.bc.zarr.ZarrArray;
import com.bc.zarr.ZarrGroup;
import com.bc.zarr.storage.FileSystemStore;
import com.bc.zarr.storage.Store;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.HashMap;
import org.apache.commons.io.FileUtils;
import ucar.ma2.InvalidRangeException;

public class ZarrStoreBuilder {

  public static class DataPoint {

    public final long longitude;
    public final long latitude;
    public final long time;
    public final int[] data;

    private DataPoint(long longitude, long latitude, long time, int[] data) {
      this.longitude = longitude;
      this.latitude = latitude;
      this.time = time;
      this.data = data;
    }
  }

  private static DataPoint[] generateTestData() {
    double lonMult = 360D / 2000D;
    double latMult = 180D / 2000D;
    DataPoint[] dataPoints = new DataPoint[2000];
    for (int i = 0; i < 2000; i++) {
      int[] data = new int[10];
      for (int d = 0; d < 10; d++) {
        data[d] = (100 * i) + d;
      }
      dataPoints[i] = new DataPoint((long) (((lonMult * (double) i) - 180D) * 10000000000000L),
          (long) (((latMult * (double) i) - 90D) * 10000000000000L), i, data);
    }
    return dataPoints;
  }

  public static TestData createTestGeoStore(Path bucketDir, String zarrKey) throws IOException, InvalidRangeException {
    Path testZarr = bucketDir.resolve(zarrKey);
    FileUtils.deleteQuietly(bucketDir.toFile());
    Files.createDirectories(testZarr.getParent());
    Store store = new FileSystemStore(testZarr);
    return createTestGeoStore(store);
  }

  public static TestData createTestGeoStore(Store store) throws IOException, InvalidRangeException {
    Compressor compNull = CompressorFactory.create("null");

    ZarrGroup root = ZarrGroup.create(
        store,
        new HashMap<String, Object>() {{
          put("groupId", 0);
        }});

    ZarrArray longitude = root.createArray(
        "longitude",
        new ArrayParams().shape(2000).chunks(10).dataType(DataType.i8).compressor(compNull),
        new HashMap<String, Object>() {{
          put("arrayId", 0);
        }}
    );
    ZarrArray latitude = root.createArray(
        "latitude",
        new ArrayParams().shape(2000).chunks(10).dataType(DataType.i8).compressor(compNull),
        new HashMap<String, Object>() {{
          put("arrayId", 1);
        }}
    );
    ZarrArray time = root.createArray("time",
        new ArrayParams().shape(2000).chunks(1000).dataType(DataType.i8).compressor(compNull),
        new HashMap<String, Object>() {{
          put("arrayId", 2);
        }}
    );
    ZarrArray data = root.createArray("data",
        new ArrayParams().shape(2000, 10).chunks(1000, 10).dataType(DataType.i4).compressor(compNull),
        new HashMap<String, Object>() {{
          put("arrayId", 3);
        }}
    );

    DataPoint[] dataPoints = generateTestData();
    for (int i = 0; i < 2000; i++) {
      DataPoint dataPoint = dataPoints[i];
      longitude.write(dataPoint.longitude, new int[]{1}, new int[]{i});
      latitude.write(dataPoint.latitude, new int[]{1}, new int[]{i});
      time.write(dataPoint.time, new int[]{1}, new int[]{i});
      data.write(dataPoint.data, new int[]{1, 10}, new int[]{i, 0});
    }

    ZarrGroup subGroup1 = root.createSubGroup("subGroup1");
    ZarrGroup subGroup11 = subGroup1.createSubGroup("subGroup11");
    ZarrGroup subGroup111 = subGroup11.createSubGroup("subGroup111");

    ZarrGroup subGroup2 = root.createSubGroup("subGroup2");
    ZarrGroup subGroup21 = subGroup2.createSubGroup("subGroup21");

    subGroup1.createArray("subGroup1Array1",
        new ArrayParams().shape(10).chunks(5).dataType(DataType.i4).compressor(compNull).fillValue(11)
    );
    subGroup1.createArray("subGroup1Array2",
        new ArrayParams().shape(10).chunks(5).dataType(DataType.i4).compressor(compNull).fillValue(12)
    );
    subGroup11.createArray("subGroup11Array1",
        new ArrayParams().shape(10).chunks(5).dataType(DataType.i4).compressor(compNull).fillValue(111)
    );
    subGroup11.createArray("subGroup11Array2",
        new ArrayParams().shape(10).chunks(5).dataType(DataType.i4).compressor(compNull).fillValue(112)
    );
    subGroup111.createArray("subGroup111Array1",
        new ArrayParams().shape(10).chunks(5).dataType(DataType.i4).compressor(compNull).fillValue(1111)
    );
    subGroup111.createArray("subGroup111Array2",
        new ArrayParams().shape(10).chunks(5).dataType(DataType.i4).compressor(compNull).fillValue(1112)
    );
    subGroup2.createArray("subGroup2Array1",
        new ArrayParams().shape(10).chunks(5).dataType(DataType.i4).compressor(compNull).fillValue(21)
    );
    subGroup2.createArray("subGroup2Array2",
        new ArrayParams().shape(10).chunks(5).dataType(DataType.i4).compressor(compNull).fillValue(22)
    );
    subGroup21.createArray("subGroup21Array1",
        new ArrayParams().shape(10).chunks(5).dataType(DataType.i4).compressor(compNull).fillValue(211)
    );
    subGroup21.createArray("subGroup21Array2",
        new ArrayParams().shape(10).chunks(5).dataType(DataType.i4).compressor(compNull).fillValue(212)
    );

    return new TestData(
        (long[]) longitude.read(),
        (long[]) latitude.read(),
        (long[]) time.read(),
        (int[]) data.read()
    );

  }

  public static class TestData {

    public final long[] longitude;
    public final long[] latitude;
    public final long[] time;
    public final int[] data;

    public TestData(long[] longitude, long[] latitude, long[] time, int[] data) {
      this.longitude = longitude;
      this.latitude = latitude;
      this.time = time;
      this.data = data;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      TestData testData = (TestData) o;
      return Arrays.equals(longitude, testData.longitude) && Arrays.equals(latitude, testData.latitude) && Arrays.equals(
          time, testData.time) && Arrays.equals(data, testData.data);
    }

    @Override
    public int hashCode() {
      int result = Arrays.hashCode(longitude);
      result = 31 * result + Arrays.hashCode(latitude);
      result = 31 * result + Arrays.hashCode(time);
      result = 31 * result + Arrays.hashCode(data);
      return result;
    }

    @Override
    public String toString() {
      return "TestData{" +
          "longitude=" + Arrays.toString(longitude) +
          ", latitude=" + Arrays.toString(latitude) +
          ", time=" + Arrays.toString(time) +
          ", data=" + Arrays.toString(data) +
          '}';
    }
  }
}
