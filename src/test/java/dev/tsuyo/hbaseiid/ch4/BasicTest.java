package dev.tsuyo.hbaseiid.ch4;

import dev.tsuyo.hbaseiid.Utils;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.jupiter.api.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.math.BigDecimal;

import static dev.tsuyo.hbaseiid.Constants.*;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class BasicTest {
  private static final Logger logger = LoggerFactory.getLogger(BasicTest.class);

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
  void testBytes() {
    byte[] byteArrayFromString = Bytes.toBytes("row");
    assertArrayEquals(new byte[]{114, 111, 119}, byteArrayFromString);
    String stringFromByteArray = Bytes.toString(byteArrayFromString);
    assertEquals("row", stringFromByteArray);

    byte[] byteArrayFromBoolean = Bytes.toBytes(true);
    assertArrayEquals(new byte[]{-1}, byteArrayFromBoolean);
    boolean booleanFromByteArray = Bytes.toBoolean(byteArrayFromBoolean);
    assertEquals(true, booleanFromByteArray);

    byte[] byteArrayFromInt = Bytes.toBytes(1);
    assertArrayEquals(new byte[]{0, 0, 0, 1}, byteArrayFromInt);
    int intFromByteArrray = Bytes.toInt(byteArrayFromInt);
    assertEquals(1, intFromByteArrray);

    byte[] byteArrayFromFloat = Bytes.toBytes(1.1f);
    assertArrayEquals(new byte[]{63, -116, -52, -51}, byteArrayFromFloat);
    float floatFromByteArrray = Bytes.toFloat(byteArrayFromFloat);
    assertEquals(1.1f, floatFromByteArrray);

    byte[] byteArrayFromBigDecimal = Bytes.toBytes(new BigDecimal(100));
    assertArrayEquals(new byte[]{0, 0, 0, 0, 100}, byteArrayFromBigDecimal);
    BigDecimal bigDecimalFromByteArray = Bytes.toBigDecimal(byteArrayFromBigDecimal);
    assertEquals(new BigDecimal(100), bigDecimalFromByteArray);
  }

  @Test
  void testGet() throws IOException {
    putSample(); // put sample data

    // get
    for (byte[] row : new byte[][]{ROWS[1], ROWS[2]}) {
      Result result = basicDao.get(mkGet(row));
      assertArrayEquals(VALS[1], result.getValue(FAM_BYTES, COLS[1]));
      assertArrayEquals(VALS[2], result.getValue(FAM_BYTES, COLS[2]));
    }
  }

  @Test
  void testExists() throws IOException {
    putSample(); // put sample data

    // exists
    for (byte[] row : new byte[][]{ROWS[1], ROWS[2]}) {
      Get get = new Get(row).addColumn(FAM_BYTES, COLS[1]);
      boolean result = basicDao.exists(get);
      assertEquals(true, result);
    }
  }

  @Test
  void testDelete() throws IOException {
    putSample(); // put sample data

    // exists (ROWS[1] & ROWS[2] should exist here)
    for (byte[] row : new byte[][]{ROWS[1], ROWS[2]}) {
      Get get = new Get(row);
      boolean result = basicDao.exists(get);
      assertEquals(true, result);
    }

    // delete
    for (byte[] row : new byte[][]{ROWS[1], ROWS[2]}) {
      Delete delete = new Delete(row).addColumn(FAM_BYTES, COLS[1]).addColumn(FAM_BYTES, COLS[2], 100L);
      basicDao.delete(delete); // atomic as PUT
    }

    // exists (ROWS[1] & ROWS[2] should NOT exist here)
    for (byte[] row : new byte[][]{ROWS[1], ROWS[2]}) {
      Get get = new Get(row);
      boolean result = basicDao.exists(get);
      assertEquals(false, result);
    }
  }

  @Test
  void testMutateRow() throws IOException {
    // mutateRow
    Mutation put = new Put(ROWS[1]).addColumn(FAM_BYTES, COLS[1], VALS[1]);
    Mutation delete = new Delete(ROWS[1]).addColumn(FAM_BYTES, COLS[1]);
    RowMutations rowMutations = new RowMutations(ROWS[1]).add(put).add(delete);
    basicDao.mutateRow(rowMutations); // atomic as PUT

    // exists (ROWS[1] should NOT exist here)
    Get get = new Get(ROWS[1]);
    boolean result = basicDao.exists(get);
    assertEquals(false, result);
  }

  // PUT sample data
  private void putSample() throws IOException {
    // put
    basicDao.put(mkPut(ROWS[1])); // PUT is atomic for a row (both col1 & col2 appear OR neither of them do)
    // put with batch
    basicDao.putBatched(mkPut(ROWS[2]));
    basicDao.flush();
  }

  private Put mkPut(byte[] row) {
    return new Put(row)
        .addColumn(FAM_BYTES, COLS[1], VALS[1])
        .addColumn(FAM_BYTES, COLS[2], 100L, VALS[2]); // 100L: timestamp
  }

  private Get mkGet(byte[] row) {
    return new Get(row)
        .addColumn(FAM_BYTES, COLS[1])
        .addColumn(FAM_BYTES, COLS[2]);
  }

}
