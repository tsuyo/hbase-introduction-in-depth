package dev.tsuyo.hbaseiid.ch7.accesscounter;

import java.util.Calendar;

public class URLAccessCount {
  private String domain;
  private String path;
  private Calendar time;
  private long count;

  public String getDomain() {
    return domain;
  }

  public void setDomain(String domain) {
    this.domain = domain;
  }

  public String getPath() {
    return path;
  }

  public void setPath(String path) {
    this.path = path;
  }

  public Calendar getTime() {
    return time;
  }

  public void setTime(Calendar time) {
    this.time = time;
  }

  public long getCount() {
    return count;
  }

  public void setCount(long count) {
    this.count = count;
  }
}
