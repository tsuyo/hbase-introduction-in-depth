package dev.tsuyo.hbaseiid.ch5;

import dev.tsuyo.hbaseiid.Constants;
import dev.tsuyo.hbaseiid.Utils;
import dev.tsuyo.hbaseiid.ch4.BasicDao;
import org.apache.hadoop.hbase.CompareOperator;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.jupiter.api.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static dev.tsuyo.hbaseiid.Constants.COLS_WITH_PREFIX;
import static dev.tsuyo.hbaseiid.Constants.FAM_BYTES;
import static dev.tsuyo.hbaseiid.Utils.getByte;
import static org.junit.jupiter.api.Assertions.*;

public class FilterTest {

  private static final Logger logger = LoggerFactory.getLogger(FilterTest.class);

  private static Connection connection;

  private BasicDao basicDao;

  @BeforeAll
  static void setup() throws IOException {
    connection = Utils.getConnection();
  }

  @AfterAll
  static void tearDown() throws IOException {
    connection.close();
  }

  @BeforeEach
  void initTable() throws IOException {
    Utils.initTable(connection);
    basicDao = new BasicDao(connection);

    putSample();
  }

  @AfterEach
  void close() throws IOException {
    basicDao.close();
  }

  @Test
  void testBasicFilter() throws IOException {
    Filter filter = new QualifierFilter(CompareOperator.EQUAL, new BinaryComparator(Constants.COLS_WITH_PREFIX[1]));
    Scan scan = new Scan().setFilter(filter);

    ResultScanner scanner = basicDao.scan(scan);
    for (Result result : scanner) {
      assertTrue(result.containsColumn(FAM_BYTES, Constants.COLS_WITH_PREFIX[1]));
      assertFalse(result.containsColumn(FAM_BYTES, Constants.COLS_WITH_PREFIX[2]));
    }
  }

  @Test
  void testTimestampsFilter() throws IOException {
    List<Long> timestamps = new ArrayList<>();
    timestamps.add(100L);
    timestamps.add(200L);

    Scan scan = new Scan().setFilter(new TimestampsFilter(timestamps));
    ResultScanner scanner = basicDao.scan(scan);
    for (Result result : scanner) {
      assertTrue(result.containsColumn(FAM_BYTES, Constants.COLS_WITH_PREFIX[1]));
      assertTrue(result.containsColumn(FAM_BYTES, Constants.COLS_WITH_PREFIX[2]));
      assertFalse(result.containsColumn(FAM_BYTES, Constants.COLS_WITH_PREFIX[3]));
    }
  }

  @Test
  void testPrefixFilter() throws IOException {
    Scan scan = new Scan().setFilter(new PrefixFilter(Bytes.toBytes("prefix1")));
    ResultScanner scanner = basicDao.scan(scan);
    for (Result result : scanner) {
      assertArrayEquals(Constants.ROWS_WITH_PREFIX[1], result.getRow());
    }
  }

  @Test
  void testColumnPrefixFilter() throws IOException {
    Scan scan = new Scan().setFilter(new ColumnPrefixFilter(Bytes.toBytes("prefix2")));
    ResultScanner scanner = basicDao.scan(scan);
    for (Result result : scanner) {
      assertFalse(result.containsColumn(FAM_BYTES, Constants.COLS_WITH_PREFIX[1]));
      assertTrue(result.containsColumn(FAM_BYTES, Constants.COLS_WITH_PREFIX[2]));
    }
  }

  @Test
  void testColumnPaginationFilter() throws IOException {
    // filter by index
    Get get1 = new Get(Constants.ROWS_WITH_PREFIX[1]).setFilter(new ColumnPaginationFilter(2, 1));
    Result result1 = basicDao.get(get1);
    assertEquals(2, result1.size()); // should match with Filter limit (= 2)
    assertFalse(result1.containsColumn(FAM_BYTES, Constants.COLS_WITH_PREFIX[0]));
    assertTrue(result1.containsColumn(FAM_BYTES, Constants.COLS_WITH_PREFIX[1]));
    assertTrue(result1.containsColumn(FAM_BYTES, Constants.COLS_WITH_PREFIX[2]));
    assertFalse(result1.containsColumn(FAM_BYTES, Constants.COLS_WITH_PREFIX[3]));

    // filter by column name
    Get get2 = new Get(Constants.ROWS_WITH_PREFIX[1]).setFilter(new ColumnPaginationFilter(2, Constants.COLS_WITH_PREFIX[1]));
    Result result2 = basicDao.get(get1);
    assertEquals(2, result2.size());
    assertFalse(result2.containsColumn(FAM_BYTES, Constants.COLS_WITH_PREFIX[0]));
    assertTrue(result2.containsColumn(FAM_BYTES, Constants.COLS_WITH_PREFIX[1]));
    assertTrue(result2.containsColumn(FAM_BYTES, Constants.COLS_WITH_PREFIX[2]));
    assertFalse(result2.containsColumn(FAM_BYTES, Constants.COLS_WITH_PREFIX[3]));
  }

  @Test
  void testColumnRangeFilter() throws IOException {
    Get get = new Get(Constants.ROWS_WITH_PREFIX[1]).setFilter(new ColumnRangeFilter(Constants.COLS_WITH_PREFIX[1], true, Constants.COLS_WITH_PREFIX[4], false));
    Result result = basicDao.get(get);
    assertFalse(result.containsColumn(FAM_BYTES, Constants.COLS_WITH_PREFIX[0]));
    assertTrue(result.containsColumn(FAM_BYTES, Constants.COLS_WITH_PREFIX[1]));
    assertTrue(result.containsColumn(FAM_BYTES, Constants.COLS_WITH_PREFIX[2]));
    assertTrue(result.containsColumn(FAM_BYTES, Constants.COLS_WITH_PREFIX[3]));
    assertFalse(result.containsColumn(FAM_BYTES, Constants.COLS_WITH_PREFIX[4]));
  }

  @Test
  void testSingleColumnValueFilter() throws IOException {
    Scan scan = new Scan();
    SingleColumnValueFilter filter =
        new SingleColumnValueFilter(
            FAM_BYTES,
            Constants.COLS_WITH_PREFIX[1],
            CompareOperator.EQUAL,
            new BinaryComparator(Bytes.toBytes("val row: prefix2_row, col: 1")));
    filter.setFilterIfMissing(true);
    filter.setLatestVersionOnly(true);
    scan.setFilter(filter);

    ResultScanner scanner = basicDao.scan(scan);
    assertNotNull(scanner.next());
    for (Result result : scanner) {
      assertArrayEquals(Constants.ROWS_WITH_PREFIX[2], result.getRow());
    }
  }

  @Test
  void testSkipFilter() throws IOException {
    Filter filter =
        new ValueFilter(
            CompareOperator.GREATER_OR_EQUAL,
            new BinaryComparator(Bytes.toBytes("val row: prefix4_row, col: 0")));
    Scan scan = new Scan().setFilter(new SkipFilter(filter));

    ResultScanner scanner = basicDao.scan(scan);
    assertNotNull(scanner.next());
    for (Result result : scanner) {
      String row = Bytes.toString(result.getRow());
      assertTrue(row.equals("prefix4_row") || row.equals("prefix5_row"));
    }
  }

  @Test
  void testWhileMatchFilter() throws IOException {
    Filter filter =
        new ValueFilter(
            CompareOperator.LESS, new BinaryComparator(Bytes.toBytes("val row: prefix4_row, col: 0")));
    Scan scan = new Scan().setFilter(new WhileMatchFilter(filter));

    ResultScanner scanner = basicDao.scan(scan);
    assertNotNull(scanner.next());
    for (Result result : scanner) {
      String row = Bytes.toString(result.getRow());
      assertTrue(
          row.equals("prefix0_row")
              || row.equals("prefix1_row")
              || row.equals("prefix2_row")
              || row.equals("prefix3_row"));
    }
  }

  @Test
  void testFilterListAll() throws IOException {
    List<Filter> filters = new ArrayList<>();
    filters.add(new QualifierFilter(CompareOperator.EQUAL, new BinaryComparator(Constants.COLS_WITH_PREFIX[1])));
    filters.add(
        new ValueFilter(
            CompareOperator.LESS, new BinaryComparator(Bytes.toBytes("val row: prefix4_row, col: 0"))));
    FilterList filterList = new FilterList(FilterList.Operator.MUST_PASS_ALL, filters);

    Scan scan = new Scan().setFilter(filterList);

    ResultScanner scanner = basicDao.scan(scan);
    assertNotNull(scanner.next());
    for (Result result : scanner) {
      String row = Bytes.toString(result.getRow());
      assertTrue(
          row.equals("prefix0_row")
              || row.equals("prefix1_row")
              || row.equals("prefix2_row")
              || row.equals("prefix3_row"));
      assertEquals(1, result.size());
      assertTrue(result.containsColumn(FAM_BYTES, Constants.COLS_WITH_PREFIX[1]));
    }
  }

  @Test
  void testFilterListOne() throws IOException {
    List<Filter> filters = new ArrayList<>();
    filters.add(new QualifierFilter(CompareOperator.EQUAL, new BinaryComparator(Constants.COLS_WITH_PREFIX[1])));
    filters.add(
        new ValueFilter(
            CompareOperator.LESS, new BinaryComparator(Bytes.toBytes("val row: prefix4_row, col: 0"))));
    FilterList filterList = new FilterList(FilterList.Operator.MUST_PASS_ONE, filters);

    Scan scan = new Scan().setFilter(filterList);

    ResultScanner scanner = basicDao.scan(scan);
    for (Result result : scanner) {
      String row = Bytes.toString(result.getRow());
      if (row.equals("prefix0_row")
          || row.equals("prefix1_row")
          || row.equals("prefix2_row")
          || row.equals("prefix3_row")) { // these matches ValueFilter
        assertEquals(COLS_WITH_PREFIX.length, result.size());
      } else {
        assertEquals(1, result.size());
        assertTrue(result.containsColumn(FAM_BYTES, Constants.COLS_WITH_PREFIX[1]));
      }
    }
  }

  private void putSample() throws IOException {
    for (byte[] row : Constants.ROWS_WITH_PREFIX) {
      Put put = new Put(row);
      for (int i = 0; i < Constants.COLS_WITH_PREFIX.length; i++) {
        put.addColumn(FAM_BYTES, Constants.COLS_WITH_PREFIX[i], i * 100L, getByte("val row: " + Bytes.toString(row) + ", col: ", i));
      }
      basicDao.put(put);
    }
  }

}
