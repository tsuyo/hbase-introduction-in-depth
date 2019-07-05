package dev.tsuyo.hbaseiid.ch4;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;

import static dev.tsuyo.hbaseiid.Constants.*;

public class BasicDao {
  private static final Logger logger = LoggerFactory.getLogger(BasicDao.class);

  private Connection connection;
  private Table table;
  private BufferedMutator btable;

  public BasicDao(Connection conn) throws IOException {
    this(conn, NS_STR, TBL_STR);
  }

  public BasicDao(Connection conn, String ns, String table) throws IOException {
    this.connection = conn;
    TableName tableName = TableName.valueOf(ns, table);
    this.table = connection.getTable(tableName);
    this.btable = connection.getBufferedMutator(tableName);
  }

  public void close() throws IOException {
    table.close();
    btable.close();
  }

  public void put(Put put) throws IOException {
    table.put(put);
  }

  public void putBatched(Put put) throws IOException {
    // obsoleted: HTableInterface.setAutoFlush()
    btable.mutate(put);
  }

  public void flush() throws IOException {
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

  public ResultScanner scan(Scan scan) throws IOException {
    return table.getScanner(scan);
  }

  public Result increment(Increment increment) throws IOException {
    return table.increment(increment);
  }

  public Result append(Append append) throws IOException {
    return table.append(append);
  }

  public Table.CheckAndMutateBuilder checkAndMutate(byte[] row, byte[] family) throws IOException {
    return table.checkAndMutate(row, family);
  }

  public void batch(final List<? extends Row> actions, final Object[] results) throws IOException, InterruptedException {
    table.batch(actions, results);
  }
}
