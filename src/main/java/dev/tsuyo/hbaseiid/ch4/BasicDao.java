package dev.tsuyo.hbaseiid.ch4;

import dev.tsuyo.hbaseiid.Utils;
import org.apache.hadoop.hbase.client.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class BasicDao {
  private static final Logger logger = LoggerFactory.getLogger(BasicDao.class);

  private Connection connection;
  private Table table;
  private BufferedMutator btable;

  public BasicDao(Connection conn) throws IOException {
    this.connection = conn;
    this.table = connection.getTable(Utils.TABLE_NAME);
    this.btable = connection.getBufferedMutator(Utils.TABLE_NAME);
  }

  public void close() throws IOException {
    table.close();
    btable.close();
  }

  public void put(Put put) throws IOException {
    table.put(put);
  }

  public void putBatched(Put put) throws IOException {
    btable.mutate(put);
    btable.flush();
  }

  public Result get(Get get) throws IOException {
    return table.get(get);
  }

  public boolean exists(Get get) throws IOException {
    return table.exists(get);
  }

  public void delete(Delete delete) throws IOException {
    table.delete(delete);
  }

  public void mutateRow(RowMutations rm) throws IOException {
    table.mutateRow(rm);
  }

}
