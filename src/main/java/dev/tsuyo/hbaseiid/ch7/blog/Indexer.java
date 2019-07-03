package dev.tsuyo.hbaseiid.ch7.blog;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;

import static dev.tsuyo.hbaseiid.ch7.blog.BlogSearchService.BLOGSEARCH_STR;
import static dev.tsuyo.hbaseiid.ch7.blog.BlogService.*;

public class Indexer {

  public int startIndex() throws Exception {
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

    Job job = Job.getInstance(conf, "blog search");
    job.setJarByClass(Indexer.class);

    Scan scan = new Scan().addFamily(D_BYTES);

    TableMapReduceUtil.initTableMapperJob(
        NS_STR + ":" + BLOG_STR,
        scan,
        IndexMapper.class,
        ImmutableBytesWritable.class,
        IntWritable.class,
        job);

    TableMapReduceUtil.initTableReducerJob(
        NS_STR + ":" + BLOGSEARCH_STR, IndexReducer.class, job);

    TableMapReduceUtil.addDependencyJars(job);

    int result = job.waitForCompletion(true) ? 0 : 1;
    // System.exit(result);
    return result;
  }

}
