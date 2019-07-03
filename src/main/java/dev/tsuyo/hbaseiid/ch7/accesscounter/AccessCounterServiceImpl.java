package dev.tsuyo.hbaseiid.ch7.accesscounter;

import dev.tsuyo.hbaseiid.Utils;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.ColumnRangeFilter;
import org.apache.hadoop.hbase.types.*;
import org.apache.hadoop.hbase.util.*;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

public class AccessCounterServiceImpl implements AccessCounterService {

  private final Connection connection;
  private final Hash hash;
  private final Struct urlRowSchema;
  private final Struct domainRowSchema;

  public AccessCounterServiceImpl() throws IOException {
    connection = Utils.getConnection();
    hash = Hash.getInstance(Hash.MURMUR_HASH3);
    urlRowSchema = new StructBuilder()
        .add(new RawInteger()) // hash of (sub-domain + root-domain)
        // TODO: ask: NULL is for dividing this string and path?
        .add(new RawStringTerminated(new byte[]{0x00})) // sub-domain + root-domain
        .add(RawString.ASCENDING) // path
        .toStruct();
    domainRowSchema = new StructBuilder()
        .add(new RawInteger()) // hash of (sub-domain + root-domain)
        .add(RawString.ASCENDING) // path
        .toStruct();
  }

  @Override
  public void count(String subDomain, String rootDomain, String path, int amount) throws IOException {
    String domain = subDomain + "." + rootDomain;
    String reversedDomain = reverseDomain(domain);

    Date date = new Date();
    SimpleDateFormat hourlyFormat = new SimpleDateFormat("yyyyMMddHH");
    SimpleDateFormat dailyFormat = new SimpleDateFormat("yyyyMMdd");

    List<Row> increments = new ArrayList<>();

    // URL
    byte[] urlRow = createURLRow(domain, path);
    Increment urlIncrement = new Increment(urlRow);
    urlIncrement.addColumn(A_FAM_H, Bytes.toBytes(hourlyFormat.format(date)), amount);
    urlIncrement.addColumn(A_FAM_D, Bytes.toBytes(dailyFormat.format(date)), amount);
    increments.add(urlIncrement);

    // Domain
    byte[] domainRow = createDomainRow(rootDomain, reversedDomain);
    Increment domainIncrement = new Increment(domainRow);
    domainIncrement.addColumn(A_FAM_H, Bytes.toBytes(hourlyFormat.format(date)), amount);
    domainIncrement.addColumn(A_FAM_D, Bytes.toBytes(dailyFormat.format(date)), amount);
    increments.add(domainIncrement);

    try (Table table = connection.getTable(TableName.valueOf(A_NS_STR, A_TABLE_STR))) {
      Object[] results = new Object[2];
      table.batch(increments, results);
    } catch (InterruptedException e) {
      // error
      throw new RuntimeException();
    }
  }

  @Override
  public List<URLAccessCount> getHourlyURLAccessCount(String subDomain, String rootDomain, String path, Calendar startHour, Calendar endHour) throws IOException {
    String domain = subDomain + "." + rootDomain;
    byte[] row = createURLRow(domain, path);
    Get get = new Get(row).addFamily(A_FAM_H);

    SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHH");
    get.setFilter(new ColumnRangeFilter(Bytes.toBytes(sdf.format(startHour.getTime())), true, Bytes.toBytes(sdf.format(endHour.getTime())), true));

    try (Table table = connection.getTable(TableName.valueOf(A_NS_STR, A_TABLE_STR))) {
      List<URLAccessCount> ret = new ArrayList<>();

      Result result = table.get(get);
      if (result.isEmpty()) {
        return ret;
      }

      for (Map.Entry<byte[], byte[]> entry : result.getFamilyMap(A_FAM_H).entrySet()) {
        byte[] yyyyMMddHH = entry.getKey();
        byte[] value = entry.getValue();

        URLAccessCount accessCount = new URLAccessCount();

        Calendar time = Calendar.getInstance();
        time.setTime(sdf.parse(Bytes.toString(yyyyMMddHH)));

        accessCount.setTime(time);
        accessCount.setDomain(domain);
        accessCount.setPath(path);
        accessCount.setCount(Bytes.toLong(value));

        ret.add(accessCount);
      }

      return ret;
    } catch (ParseException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public List<URLAccessCount> getDailyURLAccessCount(String subDomain, String rootDomain, String path, Calendar startDay, Calendar endDay) throws IOException {
    String domain = subDomain + "." + rootDomain;
    byte[] row = createURLRow(domain, path);
    Get get = new Get(row).addFamily(A_FAM_D);

    SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
    get.setFilter(new ColumnRangeFilter(Bytes.toBytes(sdf.format(startDay.getTime())), true, Bytes.toBytes(sdf.format(endDay.getTime())), true));

    try (Table table = connection.getTable(TableName.valueOf(A_NS_STR, A_TABLE_STR))) {
      List<URLAccessCount> ret = new ArrayList<>();

      Result result = table.get(get);
      if (result.isEmpty()) {
        return ret;
      }

      for (Map.Entry<byte[], byte[]> entry : result.getFamilyMap(A_FAM_D).entrySet()) {
        byte[] yyyyMMdd = entry.getKey();
        byte[] value = entry.getValue();

        URLAccessCount accessCount = new URLAccessCount();

        Calendar time = Calendar.getInstance();
        time.setTime(sdf.parse(Bytes.toString(yyyyMMdd)));

        accessCount.setTime(time);
        accessCount.setDomain(domain);
        accessCount.setPath(path);
        accessCount.setCount(Bytes.toLong(value));

        ret.add(accessCount);
      }

      return ret;
    } catch (ParseException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public List<DomainAccessCount> getHourlyDomainAccessCount(String rootDomain, Calendar startHour, Calendar endHour) throws IOException {
    String reversedRootDomain = reverseDomain(rootDomain);
    byte[] startRow = createDomainRow(rootDomain, reversedRootDomain);
    byte[] stopRow = Utils.incrementBytes(createDomainRow(rootDomain, reversedRootDomain));

    Scan scan = new Scan().withStartRow(startRow).withStopRow(stopRow).addFamily(A_FAM_H);

    SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHH");
    scan.setFilter(new ColumnRangeFilter(Bytes.toBytes(sdf.format(startHour.getTime())), true, Bytes.toBytes(sdf.format(endHour.getTime())), true));

    try (Table table = connection.getTable(TableName.valueOf(A_NS_STR, A_TABLE_STR));
         ResultScanner scanner = table.getScanner(scan)) {
      List<DomainAccessCount> ret = new ArrayList<>();

      for (Result result : scanner) {
        // reverse reverse domain = (original) domain
        String domain = reverseDomain((String) domainRowSchema.decode(new SimplePositionedByteRange(result.getRow()), 1));

        for (Map.Entry<byte[], byte[]> entry : result.getFamilyMap(A_FAM_H).entrySet()) {
          byte[] yyyyMMddHH = entry.getKey();
          byte[] value = entry.getValue();

          DomainAccessCount accessCount = new DomainAccessCount();

          Calendar time = Calendar.getInstance();
          time.setTime(sdf.parse(Bytes.toString(yyyyMMddHH)));

          accessCount.setTime(time);
          accessCount.setDomain(domain);
          accessCount.setCount(Bytes.toLong(value));

          ret.add(accessCount);
        }
      }

      return ret;
    } catch (ParseException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public List<DomainAccessCount> getDailyDomainAccessCount(String rootDomain, Calendar startDay, Calendar endDay) throws IOException {
    String reversedRootDomain = reverseDomain(rootDomain);
    byte[] startRow = createDomainRow(rootDomain, reversedRootDomain);
    byte[] stopRow = Utils.incrementBytes(createDomainRow(rootDomain, reversedRootDomain));

    Scan scan = new Scan().withStartRow(startRow).withStopRow(stopRow).addFamily(A_FAM_D);

    SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
    scan.setFilter(new ColumnRangeFilter(Bytes.toBytes(sdf.format(startDay.getTime())), true, Bytes.toBytes(sdf.format(endDay.getTime())), true));

    try (Table table = connection.getTable(TableName.valueOf(A_NS_STR, A_TABLE_STR));
         ResultScanner scanner = table.getScanner(scan)) {
      List<DomainAccessCount> ret = new ArrayList<>();

      for (Result result : scanner) {
        // reverse reverse domain = (original) domain
        String domain = reverseDomain((String) domainRowSchema.decode(new SimplePositionedByteRange(result.getRow()), 1));

        for (Map.Entry<byte[], byte[]> entry : result.getFamilyMap(A_FAM_D).entrySet()) {
          byte[] yyyyMMdd = entry.getKey();
          byte[] value = entry.getValue();

          DomainAccessCount accessCount = new DomainAccessCount();

          Calendar time = Calendar.getInstance();
          time.setTime(sdf.parse(Bytes.toString(yyyyMMdd)));

          accessCount.setTime(time);
          accessCount.setDomain(domain);
          accessCount.setCount(Bytes.toLong(value));

          ret.add(accessCount);
        }
      }

      return ret;
    } catch (ParseException e) {
      throw new RuntimeException(e);
    }
  }

  private String reverseDomain(String domain) {
    String[] split = domain.split("\\.", 2);
    if (split.length == 1) {
      return domain;
    }
    return reverseDomain(split[1]) + "." + split[0];
  }

  private byte[] createURLRow(String domain, String path) {
    byte[] domainBytes = Bytes.toBytes(domain);
    Object[] values = new Object[]{
        hash.hash(new ByteArrayHashKey(domainBytes, 0, domainBytes.length), 0),
        domain,
        path
    };
    SimplePositionedMutableByteRange positionedByteRange =
        new SimplePositionedMutableByteRange(urlRowSchema.encodedLength(values));
    urlRowSchema.encode(positionedByteRange, values);
    return positionedByteRange.getBytes();
  }

  private byte[] createDomainRow(String rootDomain, String reversedDomain) {
    byte[] rootDomainBytes = Bytes.toBytes(rootDomain);
    Object[] values = new Object[]{
        hash.hash(new ByteArrayHashKey(rootDomainBytes, 0, rootDomainBytes.length), 0),
        reversedDomain
    };
    SimplePositionedMutableByteRange positionedByteRange =
        new SimplePositionedMutableByteRange(domainRowSchema.encodedLength(values));
    domainRowSchema.encode(positionedByteRange, values);
    return positionedByteRange.getBytes();
  }
}
