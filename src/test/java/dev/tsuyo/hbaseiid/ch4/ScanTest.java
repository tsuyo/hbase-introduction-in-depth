package dev.tsuyo.hbaseiid.ch4;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static dev.tsuyo.hbaseiid.Utils.*;
import static dev.tsuyo.hbaseiid.ByteConstants.*;

import dev.tsuyo.hbaseiid.Utils;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;


// TODO: Use Utils & ByteConstants
public class ScanTest {
  private static final byte[][] ROWS = {
    get("row", 0), get("row", 1), get("row", 2), get("row", 3), get("row", 4), get("row", 5)
  };
  private static final byte[][] COLS = {
    get("col", 0), get("col", 1), get("col", 2), get("col", 3), get("col", 4)
  };
  private static final byte[][] VALS = {
    get("val", 0), get("val", 1), get("val", 2), get("val", 3), get("val", 4)
  };

  private static final Logger logger = LoggerFactory.getLogger(ScanTest.class);

  private static Connection connection;

  private static byte[] get(String s, int i) {
    return Bytes.toBytes(s + i);
  }

  @BeforeAll
  static void setup() throws IOException {
    connection = Utils.getConnectionAndInit();
  }

  void put(Table table) throws IOException {
    for (byte[] row : ROWS) {
      Put put = new Put(row);
      for (int i = 0; i < COLS.length; i++) {
        put.addColumn(FAM, COLS[i], VALS[i]);
      }
      table.put(put);
    }
  }

  void scan(Table table) throws IOException {
    Scan scan = new Scan().withStartRow(ROWS[1]).withStopRow(ROWS[3]);
    for (byte[] col : COLS) {
      scan.addColumn(FAM, col);
    }

    ResultScanner scanner = table.getScanner(scan);
    int rowi = 1;
    for (Result result : scanner) {
      assertArrayEquals(ROWS[rowi++], result.getRow());
      for (int i = 0; i < COLS.length; i++) {
        byte[] colval = result.getValue(FAM, COLS[i]);
        assertArrayEquals(VALS[i], colval);
      }
    }
  }

  void scanBatchCaching(Table table) throws IOException {
    Scan scan = new Scan().withStartRow(ROWS[1]).withStopRow(ROWS[5]).setCaching(2).setBatch(3);
    ResultScanner scanner = table.getScanner(scan);
    // setBatch(3) means the 1st Result only has col0-2 values,
    // then the 2nd Result has col3-4 values
    int cur = 0;
    for (Result result : scanner) {
      for (int i = 0; i < COLS.length; i++) {
        byte[] colval = result.getValue(FAM, COLS[i]);
        if (cur % 2 == 0) {
          if (i < 3) {
            assertArrayEquals(VALS[i], colval);
          } else {
            assertArrayEquals(null, colval);
          }
        } else {
          if (i < 3) {
            assertArrayEquals(null, colval);
          } else {
            assertArrayEquals(VALS[i], colval);
          }
        }
      }
      cur++;
    }
  }

  void scanReverse(Table table) throws IOException {
    Scan scan = new Scan().withStartRow(ROWS[4]).withStopRow(ROWS[1]).setReversed(true);
    ResultScanner scanner = table.getScanner(scan);
    int rowi = 4;
    for (Result result : scanner) {
      assertArrayEquals(ROWS[rowi--], result.getRow());
      for (int i = 0; i < COLS.length; i++) {
        byte[] colval = result.getValue(FAM, COLS[i]);
        assertArrayEquals(VALS[i], colval);
      }
    }
  }

  @Test
  void testScan() throws IOException {
    Table table = connection.getTable(TABLE_NAME);

    put(table);
    scan(table);
    scanBatchCaching(table);
    scanReverse(table);

    table.close();
  }
}
