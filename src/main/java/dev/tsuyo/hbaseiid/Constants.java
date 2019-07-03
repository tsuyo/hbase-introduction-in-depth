package dev.tsuyo.hbaseiid;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.util.Bytes;

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

  public static final byte[][] ROWS = Utils.getBytes("row", 6);
  public static final byte[][] COLS = Utils.getBytes("col", 6);
  public static final byte[][] VALS = Utils.getBytes("val", 6);
  public static final byte[][] LAVS = Utils.getBytes("lav", 6); // LAV is just the reverse of "VAL"

}
