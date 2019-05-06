package dev.tsuyo.hbaseiid.ch5;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class ColumnCounter {

  // need a following preparation on hbase side
  // TODO: these should be in JUnit @BeforeEach
  //
  // > create_namespace 'ns'
  // > create 'ns:columncount', 'fam'
  // > create 'ns:countresult', 'fam'
  // > put 'ns:columncount', 'row1', 'fam:col1', 'value1-1'
  // > put 'ns:columncount', 'row1', 'fam:col2', 'value1-2'
  // > put 'ns:columncount', 'row2', 'fam:col1', 'value2-1'
  // > put 'ns:columncount', 'row3', 'fam:col2', 'value3-2'
  // > put 'ns:columncount', 'row4', 'fam:col3', 'value4-3'
  //
  // also a jar file is required
  // $ mvn -DskipTests clean package
  //
  public static void main(String[] args) throws Exception {
    Configuration conf = HBaseConfiguration.create();
    conf.set(
        "hbase.zookeeper.quorum",
        "docker-host"); // need to set 127.0.0.1 <-> docker-host mapping in /etc/hosts
    conf.set("hbase.zookeeper.property.clientPort", "2181");
    conf.set("fs.defaultFS", "hdfs://localhost:9000");
    conf.set("mapreduce.framework.name", "yarn");
    conf.set("yarn.resourcemanager.address", "localhost:8032");
    conf.set("mapreduce.job.jar", "target/hbase-introduction-in-depth-1.0.jar");
    System.setProperty("HADOOP_USER_NAME", "hadoop");

    Job job = Job.getInstance(conf, "ColumnCounter");
    job.setJarByClass(ColumnCounter.class);

    Scan scan = new Scan();

    TableMapReduceUtil.initTableMapperJob(
        "ns:columncount",
        scan,
        ColumnCounterMapper.class,
        ImmutableBytesWritable.class,
        LongWritable.class,
        job);

    TableMapReduceUtil.initTableReducerJob("ns:countresult", ColumnCounterReducer.class, job);

    int status = job.waitForCompletion(true) ? 0 : 1;
    System.exit(status);
  }
}
