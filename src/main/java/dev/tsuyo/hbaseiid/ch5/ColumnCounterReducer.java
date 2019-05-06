package dev.tsuyo.hbaseiid.ch5;

import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class ColumnCounterReducer
    extends TableReducer<ImmutableBytesWritable, LongWritable, NullWritable> {

  @Override
  protected void reduce(ImmutableBytesWritable key, Iterable<LongWritable> values, Context context)
      throws IOException, InterruptedException {
    long count = 0;
    for (LongWritable value : values) {
      count += value.get();
    }

    Put put = new Put(key.get());
    put.addColumn(Bytes.toBytes("fam"), HConstants.EMPTY_BYTE_ARRAY, Bytes.toBytes(count));
    context.write(NullWritable.get(), put);
  }
}
