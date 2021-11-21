package edu.colorado.cires.cmg.awszarr;

import com.bc.zarr.ZarrUtils;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

class S3Path {

  private final String storeKey;
  private final List<String> parts;

  S3Path(List<String> storeKeyParts) {
    parts = storeKeyParts.stream()
        .filter(part -> part != null && !part.trim().isEmpty())
        .map(String::trim)
        .collect(Collectors.toList());
    this.storeKey = String.join("/", parts);
  }

  S3Path(String storeKey) {
    this.storeKey = storeKey.trim().isEmpty() ? "" : ZarrUtils.normalizeStoragePath(storeKey);
    parts = split(this.storeKey);
  }

  boolean endsWith(S3Path suffix) {
    int thisSize = size();
    int suffixSize = suffix.size();
    if(suffixSize > thisSize) {
      return false;
    }
    return suffix.getParts().equals(parts.subList(thisSize - suffixSize, thisSize));
  }

  private static List<String> split(String key) {
    if (key.isEmpty()) {
      return Collections.emptyList();
    }
    return Arrays.asList(key.split("/"));
  }

  public List<String> getParts() {
    return parts;
  }

  int size() {
    return parts.size();
  }

  S3Path resolve(String name) {
    if (name.trim().isEmpty()) {
      return this;
    }
    return new S3Path(storeKey + "/" + ZarrUtils.normalizeStoragePath(name));
  }



  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    S3Path s3Path = (S3Path) o;
    return Objects.equals(storeKey, s3Path.storeKey);
  }

  @Override
  public int hashCode() {
    return Objects.hash(storeKey);
  }

  @Override
  public String toString() {
    return storeKey;
  }
}
