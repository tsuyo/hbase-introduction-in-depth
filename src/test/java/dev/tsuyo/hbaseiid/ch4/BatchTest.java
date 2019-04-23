package dev.tsuyo.hbaseiid.ch4;

import dev.tsuyo.hbaseiid.Utils;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static dev.tsuyo.hbaseiid.Utils.FAM;
import static dev.tsuyo.hbaseiid.Utils.TABLE_NAME;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class BatchTest {
  private static final byte[][] ROWS = {
    get("row", 0), get("row", 1), get("row", 2), get("row", 3), get("row", 4), get("row", 5)
  };
  private static final byte[][] COLS = {
    get("col", 0), get("col", 1), get("col", 2), get("col", 3), get("col", 4)
  };
  private static final byte[][] VALS = {
    get("val", 0), get("val", 1), get("val", 2), get("val", 3), get("val", 4)
  };
  // "ANO" stands for "another"
  private static final byte[][] VALS_ANO = {
    get("anoVal", 0), get("anoVal", 1), get("anoVal", 2), get("anoVal", 3), get("anoVal", 4)
  };

  private static final Logger logger = LoggerFactory.getLogger(BatchTest.class);

  private static Connection connection;

  private static byte[] get(String s, int i) {
    return Bytes.toBytes(s + i);
  }

  @BeforeAll
  static void setup() throws IOException {
    connection = Utils.getConnection();
  }

  void put(Table table) throws IOException {
    Put put2 = new Put(ROWS[2]).addColumn(FAM, COLS[1], VALS[1]); // for GET
    Put put3 = new Put(ROWS[3]).addColumn(FAM, COLS[1], VALS[1]); // for DELETE

    table.put(put2);
    table.put(put3);
  }

  void batch(Table table) throws IOException {
    List<Row> actions = new ArrayList<>();

    Put put = new Put(ROWS[1]).addColumn(FAM, COLS[1], VALS[1]);
    actions.add(put);

    Get get = new Get(ROWS[2]).addColumn(FAM, COLS[1]);
    actions.add(get);

    Delete delete = new Delete(ROWS[3]).addColumn(FAM, COLS[1]);
    actions.add(delete);

    Increment increment = new Increment(ROWS[4]).addColumn(FAM, COLS[1], 1);
    actions.add(increment);

    Append append = new Append(ROWS[5]).addColumn(FAM, COLS[1], VALS[1]);
    actions.add(append);

    Object[] results = new Object[actions.size()];
    try {
      table.batch(actions, results);
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
              assertArrayEquals(VALS[1], result.getValue(FAM, COLS[1]));
              break;
            case 3: // Increment
              assertEquals(1L, Bytes.toLong(result.getValue(FAM, COLS[1])));
              break;
            case 4: // Append
              assertArrayEquals(VALS[1], result.getValue(FAM, COLS[1]));
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

  @Test
  void testBatch() throws IOException {
    Table table = connection.getTable(TABLE_NAME);

    put(table);
    batch(table);

    table.close();
  }
}
