package dev.tsuyo.hbaseiid.ch5;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class WordCount {

  // need a following preparation on hadoop side
  // TODO: these should be in JUnit @BeforeEach
  //
  // $ hdfs dfs -mkdir /tmp/wordcount_in
  // $ hdfs dfs -put /workspace/src/test/resources/sample.txt /tmp/wordcount_in
  // $ hdfs dfs -rm -f -r /tmp/wordcount_out
  //
  // also a jar file is required
  // $ mvn -DskipTests clean package
  //
  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    conf.set("fs.defaultFS", "hdfs://localhost:9000");
    conf.set("mapreduce.framework.name", "yarn");
    conf.set("yarn.resourcemanager.address", "localhost:8032");
    conf.set("mapreduce.job.jar", "target/hbase-introduction-in-depth-1.0.jar");
    System.setProperty("HADOOP_USER_NAME", "hadoop");

    Job job = Job.getInstance(conf, "WordCount");
    job.setJarByClass(WordCount.class);

    job.setMapperClass(WordCountMapper.class);
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(LongWritable.class);

    job.setInputFormatClass(TextInputFormat.class);
    TextInputFormat.setInputPaths(job, new Path("/tmp/wordcount_in"));

    job.setReducerClass(WordCountReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(LongWritable.class);

    job.setOutputFormatClass(TextOutputFormat.class);
    TextOutputFormat.setOutputPath(job, new Path("/tmp/wordcount_out"));

    int status = job.waitForCompletion(true) ? 0 : 1;
    System.exit(status);
  }
}
