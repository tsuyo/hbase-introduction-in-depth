package dev.tsuyo.hbaseiid.ch5;

import dev.tsuyo.hbaseiid.ColumnFamilyOption;
import dev.tsuyo.hbaseiid.Utils;
import dev.tsuyo.hbaseiid.ch4.BasicDao;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Arrays;

import static dev.tsuyo.hbaseiid.Constants.*;
import static dev.tsuyo.hbaseiid.ch5.ColumnCounter.COLUMNCOUNT_STR;
import static dev.tsuyo.hbaseiid.ch5.ColumnCounter.COUNTRESULT_STR;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;


public class ColumnCounterTest {
  private static final Logger logger = LoggerFactory.getLogger(ColumnCounterTest.class);

  private static Connection connection;
  private static BasicDao basicDaoForColumnCount;
  private static BasicDao basicDaoForCountResult;
  private static ColumnCounter columnCounter;

  @BeforeAll
  static void setup() throws IOException {
    connection = Utils.getConnection();
    basicDaoForColumnCount = new BasicDao(connection, NS_STR, COLUMNCOUNT_STR);
    basicDaoForCountResult = new BasicDao(connection, NS_STR, COUNTRESULT_STR);
    columnCounter = new ColumnCounter();
  }

  @BeforeEach
  void initTable() throws IOException {
    // create an input table
    Utils.initTable(connection, NS_STR, COLUMNCOUNT_STR, Arrays.asList(
        ColumnFamilyOption.from(FAM_STR).build()
        )
    );

    // create an output table
    Utils.initTable(connection, NS_STR, COUNTRESULT_STR, Arrays.asList(
        ColumnFamilyOption.from(FAM_STR).build()
        )
    );
  }

  @AfterAll
  static void tearDown() throws IOException {
    basicDaoForColumnCount.close();
    connection.close();
  }

  @Test
  public void testMapReduce() throws Exception {
    // Insert test data
    basicDaoForColumnCount.put(new Put(ROWS[1]).addColumn(FAM_BYTES, COLS[1], Bytes.toBytes("value1-1")));
    basicDaoForColumnCount.put(new Put(ROWS[1]).addColumn(FAM_BYTES, COLS[2], Bytes.toBytes("value1-2")));
    basicDaoForColumnCount.put(new Put(ROWS[2]).addColumn(FAM_BYTES, COLS[1], Bytes.toBytes("value2-1")));
    basicDaoForColumnCount.put(new Put(ROWS[3]).addColumn(FAM_BYTES, COLS[2], Bytes.toBytes("value3-2")));
    basicDaoForColumnCount.put(new Put(ROWS[4]).addColumn(FAM_BYTES, COLS[3], Bytes.toBytes("value4-3")));

    // Run a MapReduce
    columnCounter.startJob();

    // check the result (COUNTERRESULT table)
    // the result should be
    // (ROW) fam:col1 (COL) fam: (VAL) 2
    // (ROW) fam:col2 (COL) fam: (VAL) 2
    // (ROW) fam:col3 (COL) fam: (VAL) 1
    ResultScanner scanner = basicDaoForCountResult.scan(new Scan().addColumn(FAM_BYTES, Bytes.toBytes(":")));
    int rowi = 0;
    byte[][] expected = new byte[][]{Bytes.toBytes(2), Bytes.toBytes(2), Bytes.toBytes(1)};
    for (Result result : scanner) {
      assertArrayEquals(Bytes.toBytes(FAM_STR + ":" + "col" + (rowi + 1)), result.getRow());
      byte[] colval = result.getValue(FAM_BYTES, Bytes.toBytes(":"));
      assertArrayEquals(expected[rowi], colval);
      rowi++;
    }
  }

}
