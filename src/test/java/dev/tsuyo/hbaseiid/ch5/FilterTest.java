package dev.tsuyo.hbaseiid.ch5;

import dev.tsuyo.hbaseiid.Utils;
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

import static dev.tsuyo.hbaseiid.Utils.*;
import static org.junit.jupiter.api.Assertions.*;

public class FilterTest {
  private static final byte[][] ROWS = {
    get("row", 0), get("prefix_row", 1), get("row", 2), get("row", 3), get("row", 4), get("row", 5)
  };
  private static final byte[][] COLS = {
    get("col", 0), get("col", 1), get("prefix_col", 2), get("col", 3), get("col", 4)
  };

  private static final Logger logger = LoggerFactory.getLogger(FilterTest.class);

  private static Connection connection;
  private static Table table;

  @BeforeAll
  static void setup() throws IOException {
    connection = Utils.getConnection();
    table = connection.getTable(TABLE_NAME);
    put(table);
  }

  @AfterAll
  static void tearDown() throws IOException {
    delete(table);
  }

  static void put(Table table) throws IOException {
    for (byte[] row : ROWS) {
      Put put = new Put(row);
      for (int i = 0; i < COLS.length; i++) {
        put.addColumn(FAM, COLS[i], i * 100L, get("val row: " + Bytes.toString(row) + " col: ", i));
      }
      table.put(put);
    }
  }

  static void delete(Table table) throws IOException {
    for (byte[] row : ROWS) {
      Delete delete = new Delete(row);
      table.delete(delete);
    }
  }

  void basicFilter(Table table) throws IOException {
    Filter filter = new QualifierFilter(CompareOperator.EQUAL, new BinaryComparator(COLS[1]));
    Scan scan = new Scan().setFilter(filter);

    ResultScanner scanner = table.getScanner(scan);
    for (Result result : scanner) {
      assertTrue(result.containsColumn(FAM, COLS[1]));
      assertFalse(result.containsColumn(FAM, COLS[2]));
    }
  }

  void timestampsFilter(Table table) throws IOException {
    List<Long> timestamps = new ArrayList<>();
    timestamps.add(100L);
    timestamps.add(200L);

    Scan scan = new Scan().setFilter(new TimestampsFilter(timestamps));
    ResultScanner scanner = table.getScanner(scan);
    for (Result result : scanner) {
      assertTrue(result.containsColumn(FAM, COLS[1]));
      assertTrue(result.containsColumn(FAM, COLS[2]));
      assertFalse(result.containsColumn(FAM, COLS[3]));
    }
  }

  void prefixFilter(Table table) throws IOException {
    Scan scan = new Scan().setFilter(new PrefixFilter(Bytes.toBytes("prefix")));
    ResultScanner scanner = table.getScanner(scan);
    for (Result result : scanner) {
      assertArrayEquals(ROWS[1], result.getRow());
    }
  }

  void columnPrefixFilter(Table table) throws IOException {
    Scan scan = new Scan().setFilter(new ColumnPrefixFilter(Bytes.toBytes("prefix")));
    ResultScanner scanner = table.getScanner(scan);
    for (Result result : scanner) {
      assertFalse(result.containsColumn(FAM, COLS[1]));
      assertTrue(result.containsColumn(FAM, COLS[2]));
    }
  }

  void columnPaginationFilter(Table table) throws IOException {
    // filter by index
    Get get1 = new Get(ROWS[1]).setFilter(new ColumnPaginationFilter(2, 1));
    Result result1 = table.get(get1);
    assertEquals(2, result1.size());
    assertTrue(result1.containsColumn(FAM, COLS[1]));
    assertTrue(result1.containsColumn(FAM, COLS[3]));

    // filter by column name
    Get get2 = new Get(ROWS[1]).setFilter(new ColumnPaginationFilter(2, COLS[1]));
    Result result2 = table.get(get1);
    assertEquals(2, result2.size());
    assertTrue(result2.containsColumn(FAM, COLS[1]));
    assertTrue(result2.containsColumn(FAM, COLS[3]));
  }

  void columnRangeFilter(Table table) throws IOException {
    Get get = new Get(ROWS[1]).setFilter(new ColumnRangeFilter(COLS[1], true, COLS[4], false));
    Result result = table.get(get);
    // assertEquals(2, result.size());
    assertTrue(result.containsColumn(FAM, COLS[1]));
    assertTrue(result.containsColumn(FAM, COLS[3]));
  }

  void singleColumnValueFilter(Table table) throws IOException {
    Scan scan = new Scan();
    SingleColumnValueFilter filter =
        new SingleColumnValueFilter(
            FAM,
            COLS[1],
            CompareOperator.EQUAL,
            new BinaryComparator(Bytes.toBytes("val row: row2, col: 1")));
    filter.setFilterIfMissing(true);
    filter.setLatestVersionOnly(true);
    scan.setFilter(filter);

    ResultScanner scanner = table.getScanner(scan);
    for (Result result : scanner) {
      assertArrayEquals(ROWS[1], result.getRow());
    }
  }

  void skipFilter(Table table) throws IOException {
    Filter filter =
        new ValueFilter(
            CompareOperator.GREATER_OR_EQUAL,
            new BinaryComparator(Bytes.toBytes("val row: row4 col: 0")));
    Scan scan = new Scan().setFilter(new SkipFilter(filter));

    ResultScanner scanner = table.getScanner(scan);
    for (Result result : scanner) {
      String row = Bytes.toString(result.getRow());
      assertTrue(row.equals("row4") || row.equals("row5"));
    }
  }

  void whileMatchFilter(Table table) throws IOException {
    Filter filter =
        new ValueFilter(
            CompareOperator.LESS, new BinaryComparator(Bytes.toBytes("val row: row4 col: 0")));
    Scan scan = new Scan().setFilter(new WhileMatchFilter(filter));

    ResultScanner scanner = table.getScanner(scan);
    for (Result result : scanner) {
      String row = Bytes.toString(result.getRow());
      assertTrue(
          row.equals("prefix_row1")
              || row.equals("row0")
              || row.equals("row2")
              || row.equals("row3"));
    }
  }

  void filterListAll(Table table) throws IOException {
    List<Filter> filters = new ArrayList<>();
    filters.add(new QualifierFilter(CompareOperator.EQUAL, new BinaryComparator(COLS[1])));
    filters.add(
        new ValueFilter(
            CompareOperator.LESS, new BinaryComparator(Bytes.toBytes("val row: row4 col: 0"))));
    FilterList filterList = new FilterList(FilterList.Operator.MUST_PASS_ALL, filters);

    Scan scan = new Scan().setFilter(filterList);

    ResultScanner scanner = table.getScanner(scan);
    for (Result result : scanner) {
      String row = Bytes.toString(result.getRow());
      assertTrue(
          row.equals("prefix_row1")
              || row.equals("row0")
              || row.equals("row2")
              || row.equals("row3"));
      assertEquals(1, result.size());
      assertTrue(result.containsColumn(FAM, COLS[1]));
    }
  }

  void filterListOne(Table table) throws IOException {
    List<Filter> filters = new ArrayList<>();
    filters.add(new QualifierFilter(CompareOperator.EQUAL, new BinaryComparator(COLS[1])));
    filters.add(
        new ValueFilter(
            CompareOperator.LESS, new BinaryComparator(Bytes.toBytes("val row: row4 col: 0"))));
    FilterList filterList = new FilterList(FilterList.Operator.MUST_PASS_ONE, filters);

    Scan scan = new Scan().setFilter(filterList);

    ResultScanner scanner = table.getScanner(scan);
    for (Result result : scanner) {
      String row = Bytes.toString(result.getRow());
      if (row.equals("prefix_row1")
          || row.equals("row0")
          || row.equals("row2")
          || row.equals("row3")) {
        assertEquals(5, result.size());
      } else {
        assertEquals(1, result.size());
        assertTrue(result.containsColumn(FAM, COLS[1]));
      }
    }
  }

  @Test
  void testBasicFilter() throws IOException {
    basicFilter(table);
  }

  @Test
  void testTimestampsFilter() throws IOException {
    timestampsFilter(table);
  }

  @Test
  void testPrefixFilter() throws IOException {
    prefixFilter(table);
  }

  @Test
  void testColumnPrefixFilter() throws IOException {
    columnPrefixFilter(table);
  }

  @Test
  void testColumnPaginationFilter() throws IOException {
    columnPaginationFilter(table);
  }

  @Test
  void testColumnRangeFilter() throws IOException {
    columnRangeFilter(table);
  }

  @Test
  void testSingleColumnValueFilter() throws IOException {
    singleColumnValueFilter(table);
  }

  @Test
  void testSkipFilter() throws IOException {
    skipFilter(table);
  }

  @Test
  void testWhileMatchFilter() throws IOException {
    whileMatchFilter(table);
  }

  @Test
  void testFilterListAll() throws IOException {
    filterListAll(table);
  }

  @Test
  void testFilterListOne() throws IOException {
    filterListOne(table);
  }
}
