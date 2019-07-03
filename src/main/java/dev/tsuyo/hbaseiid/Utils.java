package dev.tsuyo.hbaseiid;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.type.TypeReference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.SortedSet;
import java.util.stream.IntStream;


public class Utils {
  public static final String ZK_HOST = "docker-host";
  public static final int ZK_PORT = 2181;
  public static final String NS_STR = "ns";
  public static final String TABLE_STR = "tbl";
  public static final String FAM_STR = "fam";
  public static final TableName TABLE_NAME = TableName.valueOf(NS_STR, TABLE_STR);

  private static final Logger logger = LoggerFactory.getLogger(Utils.class);

  public static Connection getConnectionAndInit() throws IOException {
    Connection connection = getConnection();
    initTable(connection);
    return connection;
  }

  public static Connection getConnection() throws IOException {
    return getConnection(ZK_HOST, ZK_PORT);
  }

  public static Connection getConnection(String zkHost, int zkPort) throws IOException {
    Configuration conf = HBaseConfiguration.create();
    conf.set(HConstants.ZOOKEEPER_QUORUM, zkHost); // need to set 127.0.0.1 <-> docker-host mapping in /etc/hosts
    conf.set(HConstants.ZOOKEEPER_CLIENT_PORT, Integer.toString(zkPort));
    return ConnectionFactory.createConnection(conf);
  }

  public static void initTable(Connection connection) throws IOException {
    initTable(connection, NS_STR, TABLE_STR, Arrays.asList(ColumnFamilyOption.from(FAM_STR).build()));
  }

  public static void initTable(Connection connection, String ns, String tbl, Collection<ColumnFamilyOption> cfopts) throws IOException {
    Admin admin = connection.getAdmin();

    // create namespace
    try {
      NamespaceDescriptor nd = admin.getNamespaceDescriptor(ns);
      logger.info("Namespace {} already exists.", ns);
    } catch (NamespaceNotFoundException nnfe) {
      logger.info("Creating namespace {}...", ns);
      admin.createNamespace(NamespaceDescriptor.create(ns).build());
    }

    TableName tableName = TableName.valueOf(ns + ":" + tbl);

    // delete table if exists
    if (admin.tableExists(tableName)) {
      if (admin.isTableEnabled(tableName)) {
        admin.disableTable(tableName);
      }
      logger.info("Deleting table {}...", tableName.getNameAsString());
      admin.deleteTable(tableName);
    }

    // create table
    if (!admin.tableExists(tableName)) {
      TableDescriptorBuilder tdBuilder = TableDescriptorBuilder.newBuilder(tableName);
      for (ColumnFamilyOption cfopt : cfopts) {
        ColumnFamilyDescriptorBuilder cfdBuilder = ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes(cfopt.getName()));
        cfopt.getParams().forEach((k, v) -> cfdBuilder.setValue(k, v));
        tdBuilder.setColumnFamily(cfdBuilder.build());
      }
      TableDescriptor desc = tdBuilder.build();
      logger.info("Creating table {}...", tableName.getNameAsString());
      admin.createTable(desc);
    }
  }

  public static byte[] getByte(String s, int i) {
    return Bytes.toBytes(s + i);
  }

  public static byte[][] getBytes(String prefix, int num) {
    return IntStream.range(0, num).mapToObj(i -> Bytes.toBytes("prefix" + i)).toArray(byte[][]::new);
  }

  public static <T> byte[] serialize(T obj) {
    try {
      ObjectMapper objectMapper = new ObjectMapper();
      return objectMapper.writeValueAsBytes(obj);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  public static SortedSet<Long> deserializeAsSortedSetLong(byte[] bytes) {
    try {
      ObjectMapper objectMapper = new ObjectMapper();
      return objectMapper.readValue(bytes, new TypeReference<SortedSet<Long>>() {
      });
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  public static SortedSet<Integer> deserializeAsSortedSetInteger(byte[] bytes) {
    try {
      ObjectMapper objectMapper = new ObjectMapper();
      return objectMapper.readValue(bytes, new TypeReference<SortedSet<Integer>>() {
      });
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  // Be careful - this is not a pure functional method
  // the argument bytes[] will be changed
  public static byte[] incrementBytes(byte[] bytes) {
    for (int i = 0; i < bytes.length; i++) {
      boolean increase = false;

      final int val = bytes[bytes.length - (i + 1)] & 0x0ff;
      int total = val + 1;
      if (total > 255) {
        increase = true;
        total = 0;
      }
      bytes[bytes.length - (i + 1)] = (byte) total;
      if (!increase) {
        return bytes;
      }
    }
    return bytes;
  }

}
