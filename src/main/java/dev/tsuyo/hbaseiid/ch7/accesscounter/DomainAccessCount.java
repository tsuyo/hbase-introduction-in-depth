package dev.tsuyo.hbaseiid.ch7.accesscounter;

import java.util.Calendar;

public class DomainAccessCount {
  private String domain;
  private Calendar time;
  private long count;

  public String getDomain() {
    return domain;
  }

  public void setDomain(String domain) {
    this.domain = domain;
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
