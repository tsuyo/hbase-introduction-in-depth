package dev.tsuyo.hbaseiid.ch4;

import dev.tsuyo.hbaseiid.Utils;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.jupiter.api.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static dev.tsuyo.hbaseiid.Constants.*;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class BatchTest {
  private static final Logger logger = LoggerFactory.getLogger(BatchTest.class);

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
  }

  @AfterEach
  void close() throws IOException {
    basicDao.close();
  }

  @Test
  void testBatch() throws IOException {
    putSample(); // put sample data

    List<Row> actions = new ArrayList<>();

    Put put = new Put(ROWS[1]).addColumn(FAM_BYTES, COLS[1], VALS[1]);
    actions.add(put);

    Get get = new Get(ROWS[2]).addColumn(FAM_BYTES, COLS[1]);
    actions.add(get);

    Delete delete = new Delete(ROWS[3]).addColumn(FAM_BYTES, COLS[1]);
    actions.add(delete);

    Increment increment = new Increment(ROWS[4]).addColumn(FAM_BYTES, COLS[1], 1);
    actions.add(increment);

    Append append = new Append(ROWS[5]).addColumn(FAM_BYTES, COLS[1], VALS[1]);
    actions.add(append);

    Object[] results = new Object[actions.size()];
    try {
      basicDao.batch(actions, results);
      // if Put and Delete are success, an empty Result returns
      for (int i = 0; i < results.length; i++) {
        if (results[i] instanceof Result) {
          Result result = (Result) results[i];
          switch (i) {
            case 0: // Put
            case 2: // Delete
              assertEquals(true, result.isEmpty());
              break;
            case 1: // Get
              assertArrayEquals(VALS[1], result.getValue(FAM_BYTES, COLS[1]));
              break;
            case 3: // Increment
              assertEquals(1L, Bytes.toLong(result.getValue(FAM_BYTES, COLS[1])));
              break;
            case 4: // Append
              assertArrayEquals(VALS[1], result.getValue(FAM_BYTES, COLS[1]));
              break;
          }
        }
      }
    } catch (InterruptedException e) {
      for (Object result : results) {
        if (result instanceof Exception) {
          logger.error(((Exception) result).getMessage());
        }
      }
    }
  }

  private void putSample() throws IOException {
    Put put2 = new Put(ROWS[2]).addColumn(FAM_BYTES, COLS[1], VALS[1]); // for GET
    Put put3 = new Put(ROWS[3]).addColumn(FAM_BYTES, COLS[1], VALS[1]); // for DELETE

    basicDao.put(put2);
    basicDao.put(put3);
  }

}
