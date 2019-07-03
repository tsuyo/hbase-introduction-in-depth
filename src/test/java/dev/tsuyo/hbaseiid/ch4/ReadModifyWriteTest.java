package dev.tsuyo.hbaseiid.ch4;

import dev.tsuyo.hbaseiid.Utils;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.jupiter.api.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

import static dev.tsuyo.hbaseiid.Constants.*;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class ReadModifyWriteTest {
  private static final Logger logger = LoggerFactory.getLogger(ReadModifyWriteTest.class);

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
  void testIncrement() throws IOException {
    Increment increment = new Increment(ROWS[1])
        .addColumn(FAM_BYTES, COLS[1], 1);
    Result result = basicDao.increment(increment);
    // If this col doesn't exist, just put a value instead of increment
    assertEquals(1L, Bytes.toLong(result.getValue(FAM_BYTES, COLS[1])));

    increment = new Increment(ROWS[1])
        .addColumn(FAM_BYTES, COLS[1], 2);
    result = basicDao.increment(increment);
    assertEquals(3L, Bytes.toLong(result.getValue(FAM_BYTES, COLS[1])));
  }

  @Test
  void testAppend() throws IOException {
    putSample(); // put sample data

    Append append = new Append(ROWS[1])
        .addColumn(FAM_BYTES, COLS[1], LAVS[1])
        .addColumn(FAM_BYTES, COLS[2], LAVS[2]);
    Result result = basicDao.append(append);
    // should return an "appended" value
    assertEquals(
        Bytes.toString(VALS[1]) + Bytes.toString(LAVS[1]),
        Bytes.toString(result.getValue(FAM_BYTES, COLS[1])));
    assertEquals(
        Bytes.toString(VALS[2]) + Bytes.toString(LAVS[2]),
        Bytes.toString(result.getValue(FAM_BYTES, COLS[2])));
  }

  @Test
  void testCheckAndPut() throws IOException {
    putSample(); // put sample data

    Put put = new Put(ROWS[1]).addColumn(FAM_BYTES, COLS[1], LAVS[1]);
    boolean result =
        basicDao.checkAndMutate(ROWS[1], FAM_BYTES).qualifier(COLS[1]).ifEquals(VALS[1]).thenPut(put);
    assertEquals(true, result);
    // check the value
    Get get = new Get(ROWS[1]).addColumn(FAM_BYTES, COLS[1]);
    Result getResult = basicDao.get(get);
    assertArrayEquals(LAVS[1], getResult.getValue(FAM_BYTES, COLS[1]));
  }

  @Test
  void testCheckAndDelete() throws IOException {
    putSample();

    Delete delete = new Delete(ROWS[1]);
    boolean result =
        basicDao.checkAndMutate(ROWS[1], FAM_BYTES).qualifier(COLS[1]).ifEquals(VALS[1]).thenDelete(delete);
    assertEquals(true, result);
    // check the value
    Get get = new Get(ROWS[1]);
    boolean existsResult = basicDao.exists(get);
    assertEquals(false, existsResult);
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
