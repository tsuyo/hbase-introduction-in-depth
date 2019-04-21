package dev.tsuyo.hbaseiid.ch4;

import static org.junit.jupiter.api.Assertions.*;
import static dev.tsuyo.hbaseiid.ch4.Utils.*;

import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.math.BigDecimal;

public class BasicTest {
  private static final byte[] ROW1 = Bytes.toBytes("row1");
  private static final byte[] ROW2 = Bytes.toBytes("row2");
  private static final byte[] COL1 = Bytes.toBytes("col1");
  private static final byte[] COL2 = Bytes.toBytes("col2");
  private static final byte[] VAL1 = Bytes.toBytes("val1");
  private static final byte[] VAL2 = Bytes.toBytes("val2");

  private static final Logger logger = LoggerFactory.getLogger(BasicTest.class);

  private static Connection connection;

  @BeforeAll
  static void setup() throws IOException {
    connection = Utils.getConnection();
  }

  @Test
  void testBytes() {
    byte[] byteArrayFromString = Bytes.toBytes("row");
    assertArrayEquals(new byte[] {114, 111, 119}, byteArrayFromString);
    String stringFromByteArray = Bytes.toString(byteArrayFromString);
    assertEquals("row", stringFromByteArray);

    byte[] byteArrayFromBoolean = Bytes.toBytes(true);
    assertArrayEquals(new byte[] {-1}, byteArrayFromBoolean);
    boolean booleanFromByteArray = Bytes.toBoolean(byteArrayFromBoolean);
    assertEquals(true, booleanFromByteArray);

    byte[] byteArrayFromInt = Bytes.toBytes(1);
    assertArrayEquals(new byte[] {0, 0, 0, 1}, byteArrayFromInt);
    int intFromByteArrray = Bytes.toInt(byteArrayFromInt);
    assertEquals(1, intFromByteArrray);

    byte[] byteArrayFromFloat = Bytes.toBytes(1.1f);
    assertArrayEquals(new byte[] {63, -116, -52, -51}, byteArrayFromFloat);
    float floatFromByteArrray = Bytes.toFloat(byteArrayFromFloat);
    assertEquals(1.1f, floatFromByteArrray);

    byte[] byteArrayFromBigDecimal = Bytes.toBytes(new BigDecimal(100));
    assertArrayEquals(new byte[] {0, 0, 0, 0, 100}, byteArrayFromBigDecimal);
    BigDecimal bigDecimalFromByteArray = Bytes.toBigDecimal(byteArrayFromBigDecimal);
    assertEquals(new BigDecimal(100), bigDecimalFromByteArray);
  }

  void put(Table table) throws IOException {
    Put put = new Put(ROW1);
    put.addColumn(FAM, COL1, VAL1);
    put.addColumn(FAM, COL2, 100L, VAL2);
    table.put(put);
  }

  void putBuffered(BufferedMutator btable) throws IOException {
    Put put = new Put(ROW2);
    put.addColumn(FAM, COL1, VAL1);
    put.addColumn(FAM, COL2, 100L, VAL2);
    btable.mutate(put);
    btable.flush();
  }

  void get(Table table) throws IOException {
    // for ROW1
    Get get = new Get(ROW1);
    get.addColumn(FAM, COL1);
    get.addColumn(FAM, COL2);

    Result result = table.get(get);
    byte[] col1val = result.getValue(FAM, COL1);
    byte[] col2val = result.getValue(FAM, COL2);
    assertArrayEquals(VAL1, col1val);
    assertArrayEquals(VAL2, col2val);

    // for ROW2
    get = new Get(ROW2);
    get.addColumn(FAM, COL1);
    get.addColumn(FAM, COL2);

    result = table.get(get);
    col1val = result.getValue(FAM, COL1);
    col2val = result.getValue(FAM, COL2);
    assertArrayEquals(VAL1, col1val);
    assertArrayEquals(VAL2, col2val);
  }

  void exists(Table table) throws IOException {
    Get get = new Get(ROW1);
    get.addColumn(FAM, COL1);
    boolean result = table.exists(get);
    assertEquals(true, result);
  }

  void delete(Table table) throws IOException {
    // for ROW1
    Delete delete = new Delete(ROW1);
    delete.addColumn(FAM, COL1);
    delete.addColumn(FAM, COL2, 100L);
    table.delete(delete);

    // for ROW2
    delete = new Delete(ROW2);
    delete.addColumn(FAM, COL1);
    delete.addColumn(FAM, COL2, 100L);
    table.delete(delete);
  }

  void mutateRow(Table table) throws IOException {
    Put put = new Put(ROW1);
    put.addColumn(FAM, COL1, VAL1);

    Delete delete = new Delete(ROW1);
    delete.addColumn(FAM, COL1);

    RowMutations rowMutations = new RowMutations(ROW1);
    rowMutations.add(put);
    rowMutations.add(delete);

    table.mutateRow(rowMutations);
  }

  @Test
  void testBasicOperations() throws IOException {
    Table table = connection.getTable(TABLE_NAME);
    BufferedMutator btable = connection.getBufferedMutator(TABLE_NAME);

    put(table);
    putBuffered(btable);
    get(table);
    exists(table);
    delete(table);
    mutateRow(table);

    table.close();
    btable.close();
  }
}
