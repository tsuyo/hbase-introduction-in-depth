package dev.tsuyo.hbaseiid.ch5;

import dev.tsuyo.hbaseiid.Constants;
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

import static dev.tsuyo.hbaseiid.Constants.NS_STR;

public class ColumnCounter {
  public static final String COLUMNCOUNT_STR = "columncount";
  public static final String COUNTRESULT_STR = "countresult";

  // need a following preparation on hbase side
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
  public int startJob() throws Exception {
    Configuration conf = HBaseConfiguration.create();
    conf.set(
        "hbase.zookeeper.quorum",
        "docker-host"); // need to set 127.0.0.1 <-> docker-host mapping in /etc/hosts
    conf.set("hbase.zookeeper.property.clientPort", "2181");
    conf.set("fs.defaultFS", "hdfs://localhost:9000");
    conf.set("mapreduce.framework.name", "yarn");
    conf.set("yarn.resourcemanager.address", "localhost:8032");
    conf.set("mapreduce.job.jar", "target/hbase-introduction-in-depth-1.0.jar");
    // In IntelliJ JUnits, this environment variable (HADOOP_USER_NAME) should be set as
    // Run -> Edit Configurations -> Environment Variables
    System.setProperty("HADOOP_USER_NAME", "hadoop");

    Job job = Job.getInstance(conf, "ColumnCounter");
    job.setJarByClass(ColumnCounter.class);

    Scan scan = new Scan();

    TableMapReduceUtil.initTableMapperJob(
        NS_STR + ":" + COLUMNCOUNT_STR,
        scan,
        ColumnCounterMapper.class,
        ImmutableBytesWritable.class,
        LongWritable.class,
        job);

    TableMapReduceUtil.initTableReducerJob(NS_STR + ":" + COUNTRESULT_STR, ColumnCounterReducer.class, job);

    return job.waitForCompletion(true) ? 0 : 1;
  }

  public static void main(String[] args) throws Exception {
    System.exit(new ColumnCounter().startJob());
  }
}
