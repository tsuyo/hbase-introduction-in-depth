package dev.tsuyo.hbaseiid;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class ColumnFamilyOption {
  private final String name;
  private final Map<String, String> params;

  public static class Builder {
    // required parameters
    private final String name;

    // optional parameters
    private Map<String, String> params = new HashMap<>();

    public Builder(String n) {
      this.name = n;
    }

    public Builder addParameter(String key, String value) {
      params.put(key, value);
      return this;
    }

    public ColumnFamilyOption build() {
      return new ColumnFamilyOption(this);
    }
  }

  private ColumnFamilyOption(Builder builder) {
    this.name = builder.name;
    this.params = Collections.unmodifiableMap(builder.params);
  }

  public static Builder from(String n) {
    return new Builder(n);
  }

  public String getName() {
    return name;
  }

  public Map<String, String> getParams() {
    return params;
  }

  @Override
  public String toString() {
    return "ColumnFamilyOption{" +
        "name='" + name + '\'' +
        ", params=" + params +
        '}';
  }
}
