package dev.tsuyo.hbaseiid.ch7.accesscounter;

import dev.tsuyo.hbaseiid.ColumnFamilyOption;
import dev.tsuyo.hbaseiid.Utils;
import org.apache.hadoop.hbase.client.Connection;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Arrays;
import java.util.Calendar;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;


public class AccessCounterTest {
  private static final Logger logger = LoggerFactory.getLogger(AccessCounterTest.class);

  private static Connection connection;
  private static AccessCounterService accessCounterService;

  @BeforeAll
  static void setup() throws IOException {
    connection = Utils.getConnection();
    accessCounterService = new AccessCounterServiceImpl();
  }

  @BeforeEach
  void initTable() throws IOException {
    Utils.initTable(connection, Utils.NS_STR, "access", Arrays.asList(
        ColumnFamilyOption.from("d")
            .addParameter("TTL", "31536000")
            .addParameter("BLOOMFILTER", "ROW")
            .build(),
        ColumnFamilyOption.from("h")
            .addParameter("TTL", "2592000")
            .addParameter("BLOOMFILTER", "ROW")
            .build())
    );
  }

  @AfterAll
  static void tearDown() throws IOException {
    connection.close();
  }

  private void simulateAccess() throws IOException {
    accessCounterService.count("blog", "tsuyo.dev", "1", 1);
    accessCounterService.count("blog", "tsuyo.dev", "2", 2);
    accessCounterService.count("blog", "tsuyo.dev", "3", 3);
    accessCounterService.count("blog", "tsuyo.dev", "1", 4);
    accessCounterService.count("www", "tsuyo.dev", "1", 11);
  }

  // TODO: Tests should consider dates & times
  @Test
  public void testGetHourlyURLAccessCount() throws IOException {
    Calendar before = Calendar.getInstance();
    simulateAccess();
    Calendar after = Calendar.getInstance();

    List<URLAccessCount> urlAccessCounts = accessCounterService.getHourlyURLAccessCount("blog", "tsuyo.dev", "1", before, after);
    assertEquals(1, urlAccessCounts.size());
    assertEquals(5, urlAccessCounts.get(0).getCount());
    assertEquals("blog.tsuyo.dev", urlAccessCounts.get(0).getDomain());
    assertEquals("1", urlAccessCounts.get(0).getPath());
  }

  @Test
  public void testGetDailyURLAccessCount() throws IOException {
    Calendar before = Calendar.getInstance();
    simulateAccess();
    Calendar after = Calendar.getInstance();

    List<URLAccessCount> urlAccessCounts = accessCounterService.getDailyURLAccessCount("blog", "tsuyo.dev", "1", before, after);
    assertEquals(1, urlAccessCounts.size());
    assertEquals(5, urlAccessCounts.get(0).getCount());
    assertEquals("blog.tsuyo.dev", urlAccessCounts.get(0).getDomain());
    assertEquals("1", urlAccessCounts.get(0).getPath());
  }

  @Test
  public void testGetHourlyDomainAccessCount() throws IOException {
    Calendar before = Calendar.getInstance();
    simulateAccess();
    Calendar after = Calendar.getInstance();

    List<DomainAccessCount> domainAccessCounts = accessCounterService.getHourlyDomainAccessCount("tsuyo.dev", before, after);
    assertEquals(2, domainAccessCounts.size());
    assertEquals(10, domainAccessCounts.get(0).getCount()); // blog.tsuyo.dev (per sub-domain)
    assertEquals("blog.tsuyo.dev", domainAccessCounts.get(0).getDomain());
    assertEquals(11, domainAccessCounts.get(1).getCount()); // www.tsuyo.dev (per sub-domain)
    assertEquals("www.tsuyo.dev", domainAccessCounts.get(1).getDomain());
  }

  @Test
  public void testGetDailyDomainAccessCount() throws IOException {
    Calendar before = Calendar.getInstance();
    simulateAccess();
    Calendar after = Calendar.getInstance();

    List<DomainAccessCount> domainAccessCounts = accessCounterService.getDailyDomainAccessCount("tsuyo.dev", before, after);
    assertEquals(2, domainAccessCounts.size());
    assertEquals(10, domainAccessCounts.get(0).getCount()); // blog.tsuyo.dev (per sub-domain)
    assertEquals("blog.tsuyo.dev", domainAccessCounts.get(0).getDomain());
    assertEquals(11, domainAccessCounts.get(1).getCount()); // www.tsuyo.dev (per sub-domain)
    assertEquals("www.tsuyo.dev", domainAccessCounts.get(1).getDomain());
  }
}
