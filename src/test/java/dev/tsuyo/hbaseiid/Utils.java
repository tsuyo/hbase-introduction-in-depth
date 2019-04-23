package dev.tsuyo.hbaseiid;

import dev.tsuyo.hbaseiid.ch4.BasicTest;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.NamespaceNotFoundException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class Utils {
  public static final String NS = "ns";
  public static final String TABLE = "tbl";
  public static final TableName TABLE_NAME = TableName.valueOf(NS + ":" + TABLE);
  public static final byte[] FAM = Bytes.toBytes("fam");

  private static final Logger logger = LoggerFactory.getLogger(BasicTest.class);

  public static Connection getConnection() throws IOException {
    Configuration conf = HBaseConfiguration.create();
    conf.set(
        "hbase.zookeeper.quorum",
        "docker-host"); // need to set 127.0.0.1 <-> docker-host mapping in /etc/hosts
    conf.set("hbase.zookeeper.property.clientPort", "2181");
    Connection connection = ConnectionFactory.createConnection(conf);

    Admin admin = connection.getAdmin();

    try {
      NamespaceDescriptor nd = admin.getNamespaceDescriptor(NS);
      logger.info("Namespace already exists. Just skip the rest..");
    } catch (NamespaceNotFoundException nnfe) {
      admin.createNamespace(NamespaceDescriptor.create(NS).build());
    }

    // force delete tables
    if (admin.tableExists(TABLE_NAME)) {
      logger.info("Deleting {}.", TABLE_NAME.getNameAsString());
      if (admin.isTableEnabled(TABLE_NAME)) {
        admin.disableTable(TABLE_NAME);
      }
      admin.deleteTable(TABLE_NAME);
    }

    // create table
    if (admin.tableExists(TABLE_NAME)) {
      logger.info("Table already exists.");
    } else {
      logger.info("Creating table...");
      TableDescriptorBuilder tdBuilder = TableDescriptorBuilder.newBuilder(TABLE_NAME);
      ColumnFamilyDescriptorBuilder cfdBuilder = ColumnFamilyDescriptorBuilder.newBuilder(FAM);
      tdBuilder.setColumnFamily(cfdBuilder.build());
      TableDescriptor desc = tdBuilder.build();
      admin.createTable(desc);
      logger.info("Table created.");
    }

    return connection;
  }

  public static byte[] get(String s, int i) {
    return Bytes.toBytes(s + i);
  }
}
