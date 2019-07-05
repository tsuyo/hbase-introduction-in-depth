package dev.tsuyo.hbaseiid.ch6;

import dev.tsuyo.hbaseiid.Utils;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.security.SecureRandom;
import java.util.SortedSet;

import static dev.tsuyo.hbaseiid.Constants.*;

public class CasDao {
  private static final Logger logger = LoggerFactory.getLogger(CasDao.class);

  private Connection connection;
  private Table table;

  public CasDao(Connection conn) throws IOException {
    this.connection = conn;
    this.table = connection.getTable(NS_TBL_TABLE);
  }

  public void close() throws IOException {
    table.close();
  }

  public boolean checkAndPut() throws IOException {
    Put put = new Put(ROW_BYTES).addColumn(FAM_BYTES, COL_BYTES, VAL_BYTES);
    return table.checkAndMutate(ROW_BYTES, FAM_BYTES).qualifier(COL_BYTES).ifNotExists().thenPut(put);
  }

  public void putWithoutConflict() throws IOException {
    SecureRandom random = new SecureRandom();

    while (true) {
      logger.info("trying to put w/o conflict...");
      Get get = new Get(ROW_BYTES).addColumn(FAM_BYTES, COL_BYTES);

      Result result = table.get(get);
      byte[] oldValue = result.getValue(FAM_BYTES, COL_BYTES);

      SortedSet<Long> set = Utils.deserializeAsSortedSetLong(oldValue);
      set.add(random.nextLong());

      Put put = new Put(ROW_BYTES).addColumn(FAM_BYTES, COL_BYTES, Utils.serialize(set));

      if (table.checkAndMutate(ROW_BYTES, FAM_BYTES).qualifier(COL_BYTES).ifEquals(oldValue).thenPut(put)) {
        break;
      }
    }
  }

  public void putMultiColsWithoutConflict() throws IOException {
    SecureRandom random = new SecureRandom();

    while (true) {
      logger.info("trying to put multi cols w/o conflict...");
      byte[] updateNum = Bytes.toBytes("update_num");
      Get get = new Get(ROW_BYTES).addColumn(FAM_BYTES, updateNum).addColumn(FAM_BYTES, COLS[1]).addColumn(FAM_BYTES, COLS[2]);

      Result result = table.get(get);
      byte[] oldUpdateNum = result.getValue(FAM_BYTES, updateNum);
      SortedSet<Long> set1 = Utils.deserializeAsSortedSetLong(result.getValue(FAM_BYTES, COLS[1]));
      set1.add(random.nextLong());
      SortedSet<Long> set2 = Utils.deserializeAsSortedSetLong(result.getValue(FAM_BYTES, COLS[2]));
      set2.add(random.nextLong());

      Put put = new Put(ROW_BYTES)
          .addColumn(FAM_BYTES, updateNum, Bytes.incrementBytes(oldUpdateNum, 1))
          .addColumn(FAM_BYTES, COLS[1], Utils.serialize(set1))
          .addColumn(FAM_BYTES, COLS[2], Utils.serialize(set2));

      // check updateNum is intact
      if (table.checkAndMutate(ROW_BYTES, FAM_BYTES).qualifier(updateNum).ifEquals(oldUpdateNum).thenPut(put)) {
        break;
      }
    }
  }

}
