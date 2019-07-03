package dev.tsuyo.hbaseiid.ch4;

import dev.tsuyo.hbaseiid.Utils;
import org.apache.hadoop.hbase.client.*;
import org.junit.jupiter.api.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

import static dev.tsuyo.hbaseiid.Constants.*;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;

public class ScanTest {
  private static final Logger logger = LoggerFactory.getLogger(ScanTest.class);

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
    putSample(); // put sample data
  }

  @AfterEach
  void close() throws IOException {
    basicDao.close();
  }

  @Test
  void testScan() throws IOException {
    Scan scan = new Scan().withStartRow(ROWS[1]).withStopRow(ROWS[3]);
    for (byte[] col : COLS) {
      scan.addColumn(FAM_BYTES, col);
    }

    ResultScanner scanner = basicDao.scan(scan);
    int rowi = 1;
    for (Result result : scanner) {
      assertArrayEquals(ROWS[rowi++], result.getRow());
      for (int i = 0; i < COLS.length; i++) {
        byte[] colval = result.getValue(FAM_BYTES, COLS[i]);
        assertArrayEquals(VALS[i], colval);
      }
    }
  }

  @Test
  void scanBatchCaching() throws IOException {
    // setCaching(): how many Result class caches (retrieves) per 1 communication to RegionServer
    // setBatch(): how many columns 1 Result class has per 1 communication to RegionServer
    Scan scan = new Scan().withStartRow(ROWS[1]).withStopRow(ROWS[5]).setCaching(2).setBatch(3);
    ResultScanner scanner = basicDao.scan(scan);
    // setBatch(3) means the 1st Result only has col0-2 values,
    // then the 2nd Result has col3-4 values
    int cur = 0;
    for (Result result : scanner) {
      for (int i = 0; i < COLS.length; i++) {
        byte[] colval = result.getValue(FAM_BYTES, COLS[i]);
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

  @Test
  void scanReverse() throws IOException {
    Scan scan = new Scan().withStartRow(ROWS[4]).withStopRow(ROWS[1]).setReversed(true);
    ResultScanner scanner = basicDao.scan(scan);
    int rowi = 4;
    for (Result result : scanner) {
      assertArrayEquals(ROWS[rowi--], result.getRow());
      for (int i = 0; i < COLS.length; i++) {
        byte[] colval = result.getValue(FAM_BYTES, COLS[i]);
        assertArrayEquals(VALS[i], colval);
      }
    }
  }

  private void putSample() throws IOException {
    for (byte[] row : ROWS) {
      Put put = new Put(row);
      for (int i = 0; i < COLS.length; i++) {
        put.addColumn(FAM_BYTES, COLS[i], VALS[i]);
      }
      basicDao.put(put);
    }
  }

}
