package dev.tsuyo.hbaseiid;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.NamespaceNotFoundException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.type.TypeReference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.SortedSet;
import java.util.stream.IntStream;

public class Utils {
  public static final String ZK_HOST = "docker-host";
  public static final int ZK_PORT = 2181;
  public static final String NS_STR = "ns";
  public static final String TABLE_STR = "tbl";
  public static final String FAM_STR = "fam";
  public static final TableName TABLE_NAME = TableName.valueOf(NS_STR + ":" + TABLE_STR);

  private static final Logger logger = LoggerFactory.getLogger(Utils.class);

  public static Connection getConnectionAndInit() throws IOException {
    Connection connection = getConnection(ZK_HOST, ZK_PORT);
    initTable(connection, NS_STR, TABLE_STR, FAM_STR);
    return connection;
  }

  public static Connection getConnection(String zkHost, int zkPort) throws IOException {
    Configuration conf = HBaseConfiguration.create();
    conf.set("hbase.zookeeper.quorum", zkHost); // need to set 127.0.0.1 <-> docker-host mapping in /etc/hosts
    conf.set("hbase.zookeeper.property.clientPort", Integer.toString(zkPort));
    return ConnectionFactory.createConnection(conf);
  }

  public static void initTable(Connection connection) throws IOException {
    initTable(connection, NS_STR, TABLE_STR, FAM_STR);
  }

  public static void initTable(Connection connection, String ns, String tbl, String fam) throws IOException {
    Admin admin = connection.getAdmin();

    // create a namespace
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
      ColumnFamilyDescriptorBuilder cfdBuilder = ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes(fam));
      tdBuilder.setColumnFamily(cfdBuilder.build());
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

  public static byte[] serialize(SortedSet<Long> set) {
    try {
      ObjectMapper objectMapper = new ObjectMapper();
      return objectMapper.writeValueAsBytes(set);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  public static SortedSet<Long> deserialize(byte[] bytes) {
    try {
      ObjectMapper objectMapper = new ObjectMapper();
      return objectMapper.readValue(bytes, new TypeReference<SortedSet<Long>>() {
      });
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
}
