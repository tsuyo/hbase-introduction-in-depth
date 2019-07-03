package dev.tsuyo.hbaseiid.ch7.accesscounter;

import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.Calendar;
import java.util.List;

public interface AccessCounterService {
  public static final String A_NS_STR = "ns";
  public static final String A_TABLE_STR = "access";
  public static final byte[] A_FAM_H = Bytes.toBytes("h");
  public static final byte[] A_FAM_D = Bytes.toBytes("d");
  // public static final byte[] A_MESSAGE_ID = Bytes.toBytes("messageId");
  // public static final byte[] A_USER_ID = Bytes.toBytes("userId");
  // public static final byte[] A_BODY = Bytes.toBytes("body");

  // count access
  void count(String subDomain, String rootDomain, String path, int amount) throws IOException;

  List<URLAccessCount> getHourlyURLAccessCount(String subDomain, String rootDomain, String path,
                                               Calendar startHour, Calendar endHour) throws IOException;

  List<URLAccessCount> getDailyURLAccessCount(String subDomain, String rootDomain, String path,
                                              Calendar startDay, Calendar endDay) throws IOException;

  List<DomainAccessCount> getHourlyDomainAccessCount(String rootDomain,
                                                     Calendar startHour, Calendar endHour) throws IOException;

  List<DomainAccessCount> getDailyDomainAccessCount(String rootDomain,
                                                    Calendar startDay, Calendar endDay) throws IOException;
}
