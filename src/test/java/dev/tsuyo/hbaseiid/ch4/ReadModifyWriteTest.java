package dev.tsuyo.hbaseiid.ch4;

import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

import static dev.tsuyo.hbaseiid.ch4.Utils.FAM;
import static dev.tsuyo.hbaseiid.ch4.Utils.TABLE_NAME;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class ReadModifyWriteTest {
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

  private static final Logger logger = LoggerFactory.getLogger(ReadModifyWriteTest.class);

  private static Connection connection;

  private static byte[] get(String s, int i) {
    return Bytes.toBytes(s + i);
  }

  @BeforeAll
  static void setup() throws IOException {
    connection = Utils.getConnection();
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

  void delete(Table table) throws IOException {
    Delete delete = new Delete(ROWS[1]);
    table.delete(delete);
  }

  void increment(Table table) throws IOException {
    Increment increment = new Increment(ROWS[1]);
    increment.addColumn(FAM, COLS[1], 1);
    Result result = table.increment(increment);
    assertEquals(1L, Bytes.toLong(result.getValue(FAM, COLS[1])));

    increment = new Increment(ROWS[1]);
    increment.addColumn(FAM, COLS[1], 2);
    result = table.increment(increment);
    assertEquals(3L, Bytes.toLong(result.getValue(FAM, COLS[1])));
  }

  void append(Table table) throws IOException {
    Append append = new Append(ROWS[1]);
    append.addColumn(FAM, COLS[1], VALS_ANO[1]);
    append.addColumn(FAM, COLS[2], VALS_ANO[2]);
    Result result = table.append(append);
    assertEquals(
        Bytes.toString(VALS[1]) + Bytes.toString(VALS_ANO[1]),
        Bytes.toString(result.getValue(FAM, COLS[1])));
    assertEquals(
        Bytes.toString(VALS[2]) + Bytes.toString(VALS_ANO[2]),
        Bytes.toString(result.getValue(FAM, COLS[2])));
  }

  void checkAndPut(Table table) throws IOException {
    Put put = new Put(ROWS[1]).addColumn(FAM, COLS[1], VALS_ANO[1]);
    boolean result =
        table.checkAndMutate(ROWS[1], FAM).qualifier(COLS[1]).ifEquals(VALS[1]).thenPut(put);
    assertEquals(true, result);
    // check the value
    Get get = new Get(ROWS[1]).addColumn(FAM, COLS[1]);
    Result getResult = table.get(get);
    assertArrayEquals(VALS_ANO[1], getResult.getValue(FAM, COLS[1]));
  }

  void checkAndDelete(Table table) throws IOException {
    Delete delete = new Delete(ROWS[1]);
    boolean result =
        table.checkAndMutate(ROWS[1], FAM).qualifier(COLS[1]).ifEquals(VALS[1]).thenDelete(delete);
    assertEquals(true, result);
    // check the value
    Get get = new Get(ROWS[1]);
    boolean existsResult = table.exists(get);
    assertEquals(false, existsResult);
  }

  @Test
  void testIncrement() throws IOException {
    Table table = connection.getTable(TABLE_NAME);

    increment(table);

    table.close();
  }

  @Test
  void testAppend() throws IOException {
    Table table = connection.getTable(TABLE_NAME);

    delete(table);
    put(table);
    append(table);

    table.close();
  }

  @Test
  void testCheckAndPut() throws IOException {
    Table table = connection.getTable(TABLE_NAME);

    delete(table);
    put(table);
    checkAndPut(table);

    table.close();
  }

  @Test
  void testCheckAndDelete() throws IOException {
    Table table = connection.getTable(TABLE_NAME);

    delete(table);
    put(table);
    checkAndDelete(table);

    table.close();
  }
}
