package dev.tsuyo.hbaseiid.ch6;

import dev.tsuyo.hbaseiid.Utils;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.security.SecureRandom;
import java.util.SortedSet;

import static dev.tsuyo.hbaseiid.ByteConstants.*;

public class CasDao {
  private static final Logger logger = LoggerFactory.getLogger(CasDao.class);

  private Connection connection;
  private Table table;

  public CasDao(Connection conn) throws IOException {
    this.connection = conn;
    this.table = connection.getTable(Utils.TABLE_NAME);
  }

  public void close() throws IOException {
    table.close();
  }

  public boolean checkAndPut() throws IOException {
    Put put = new Put(ROW).addColumn(FAM, COL, VAL);
    return table.checkAndMutate(ROW, FAM).qualifier(COL).ifNotExists().thenPut(put);
  }

  public void putWithoutConflict() throws IOException {
    SecureRandom random = new SecureRandom();

    while (true) {
      logger.info("trying to put w/o conflict...");
      Get get = new Get(ROW).addColumn(FAM, COL);

      Result result = table.get(get);
      byte[] oldValue = result.getValue(FAM, COL);

      SortedSet<Long> set = Utils.deserializeAsSortedSetLong(oldValue);
      set.add(random.nextLong());

      Put put = new Put(ROW).addColumn(FAM, COL, Utils.serialize(set));

      if (table.checkAndMutate(ROW, FAM).qualifier(COL).ifEquals(oldValue).thenPut(put)) {
        break;
      }
    }
  }

  public void putMultiColsWithoutConflict() throws IOException {
    SecureRandom random = new SecureRandom();

    while (true) {
      logger.info("trying to put multi cols w/o conflict...");
      byte[] updateNum = Bytes.toBytes("update_num");
      Get get = new Get(ROW).addColumn(FAM, updateNum).addColumn(FAM, COLS[1]).addColumn(FAM, COLS[2]);

      Result result = table.get(get);
      byte[] oldUpdateNum = result.getValue(FAM, updateNum);
      SortedSet<Long> set1 = Utils.deserializeAsSortedSetLong(result.getValue(FAM, COLS[1]));
      set1.add(random.nextLong());
      SortedSet<Long> set2 = Utils.deserializeAsSortedSetLong(result.getValue(FAM, COLS[2]));
      set2.add(random.nextLong());

      Put put = new Put(ROW)
          .addColumn(FAM, updateNum, Bytes.incrementBytes(oldUpdateNum, 1))
          .addColumn(FAM, COLS[1], Utils.serialize(set1))
          .addColumn(FAM, COLS[2], Utils.serialize(set2));

      if (table.checkAndMutate(ROW, FAM).qualifier(updateNum).ifEquals(oldUpdateNum).thenPut(put)) {
        break;
      }
    }
  }

}
