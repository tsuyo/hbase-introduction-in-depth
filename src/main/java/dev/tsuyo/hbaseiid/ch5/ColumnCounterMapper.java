package dev.tsuyo.hbaseiid.ch5;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;

import java.io.IOException;
import java.util.Map;
import java.util.NavigableMap;

public class ColumnCounterMapper extends TableMapper<ImmutableBytesWritable, LongWritable> {

  @Override
  protected void map(ImmutableBytesWritable key, Result value, Context context)
      throws IOException, InterruptedException {
    for (Map.Entry<byte[], NavigableMap<byte[], byte[]>> familyEntry :
        value.getNoVersionMap().entrySet()) {
      byte[] family = familyEntry.getKey();
      for (byte[] qualifier : familyEntry.getValue().keySet()) {
        byte[] column = Bytes.add(family, Bytes.toBytes(":"), qualifier);
        context.write(new ImmutableBytesWritable(column), new LongWritable(1));
      }
    }
  }
}
