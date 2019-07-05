package dev.tsuyo.hbaseiid.ch6;

import dev.tsuyo.hbaseiid.Utils;
import dev.tsuyo.hbaseiid.ch4.BasicDao;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.jupiter.api.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.TreeSet;

import static dev.tsuyo.hbaseiid.Constants.*;
import static org.junit.jupiter.api.Assertions.*;


public class CasTest {
  private static final Logger logger = LoggerFactory.getLogger(CasTest.class);

  private static Connection connection;

  private BasicDao basicDao;
  private CasDao casDao;

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
    casDao = new CasDao(connection);
  }

  @AfterEach
  void close() throws IOException {
    basicDao.close();
    casDao.close();
  }

  @Test
  void testCheckAndMutate() throws IOException {
    assertTrue(casDao.checkAndPut());  // 1st CAS (check & put) succeed
    assertFalse(casDao.checkAndPut()); // 2nd (and further try) fails
  }

  // TODO: from here
  @Test
  void testMutateWithoutConflict() throws IOException {
    // put a sample data
    basicDao.put(new Put(ROW_BYTES).addColumn(FAM_BYTES, COL_BYTES, Utils.serialize(new TreeSet<Long>())));

    casDao.putWithoutConflict();

    // check
    Result result = basicDao.get(new Get(ROW_BYTES).addColumn(FAM_BYTES, COL_BYTES));
    assertEquals(1, Utils.deserializeAsSortedSetLong(result.getValue(FAM_BYTES, COL_BYTES)).size());
  }

  @Test
  void testPutMultiColsWithoutConflict() throws IOException {
    // put a sample data
    byte[] updateNum = Bytes.toBytes("update_num");
    basicDao.put(
        new Put(ROW_BYTES)
            .addColumn(FAM_BYTES, updateNum, Bytes.toBytes(0))
            .addColumn(FAM_BYTES, COLS[1], Utils.serialize(new TreeSet<Long>()))
            .addColumn(FAM_BYTES, COLS[2], Utils.serialize(new TreeSet<Long>()))
    );

    casDao.putMultiColsWithoutConflict();

    // check
    Result result = basicDao.get(new Get(ROW_BYTES).addColumn(FAM_BYTES, COLS[1]).addColumn(FAM_BYTES, COLS[2]));
    assertEquals(1, Utils.deserializeAsSortedSetLong(result.getValue(FAM_BYTES, COLS[1])).size());
    assertEquals(1, Utils.deserializeAsSortedSetLong(result.getValue(FAM_BYTES, COLS[2])).size());
  }

}
