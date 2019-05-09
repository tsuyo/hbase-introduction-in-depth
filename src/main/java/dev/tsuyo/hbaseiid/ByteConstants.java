package dev.tsuyo.hbaseiid;

import org.apache.hadoop.hbase.util.Bytes;

public class ByteConstants {
  public static final byte[] FAM = Bytes.toBytes("fam");

  public static final byte[] ROW = Bytes.toBytes("row");
  public static final byte[][] ROWS = Utils.getBytes("row", 6);

  public static final byte[] COL = Bytes.toBytes("col");
  public static final byte[][] COLS = Utils.getBytes("col", 6);

  public static final byte[] VAL = Bytes.toBytes("val");
  public static final byte[][] VALS = Utils.getBytes("val", 6);
}
