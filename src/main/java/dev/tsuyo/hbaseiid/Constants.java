package dev.tsuyo.hbaseiid;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.util.Bytes;

import static dev.tsuyo.hbaseiid.Utils.getByte;

public class Constants {
  public static final String PROP_ZK_HOST = "docker-host";
  public static final int PROP_ZK_PORT = 2181;

  public static final String NS_STR = "ns";
  public static final String FAM_STR = "fam";
  public static final String TBL_STR = "tbl";

  public static final TableName NS_TBL_TABLE = TableName.valueOf(NS_STR, TBL_STR);

  public static final byte[] FAM_BYTES = Bytes.toBytes("fam");
  public static final byte[] ROW_BYTES = Bytes.toBytes("row");
  public static final byte[] COL_BYTES = Bytes.toBytes("col");
  public static final byte[] VAL_BYTES = Bytes.toBytes("val");

  public static final byte[][] ROWS = Utils.getBytes("row%d", 6);
  public static final byte[][] COLS = Utils.getBytes("col%d", 6);
  public static final byte[][] VALS = Utils.getBytes("val%d", 6);
  public static final byte[][] LAVS = Utils.getBytes("lav%d", 6); // LAV is just the reverse of "VAL"
  //  public static final byte[][] ROWS_WITH_PREFIX = {
//      getByte("row", 0), getByte("prefix_row", 1), getByte("row", 2), getByte("row", 3), getByte("row", 4), getByte("row", 5)
//  };
//  public static final byte[][] COLS_WITH_PREFIX = {
//      getByte("col", 0), getByte("col", 1), getByte("prefix_col", 2), getByte("col", 3), getByte("col", 4)
//  };
  public static final byte[][] ROWS_WITH_PREFIX = Utils.getBytes("prefix%d_row", 6);
  public static final byte[][] COLS_WITH_PREFIX = Utils.getBytes("prefix%d_col", 6);

}
