package dev.tsuyo.hbaseiid.ch7.blog;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.types.RawInteger;
import org.apache.hadoop.hbase.types.RawLong;
import org.apache.hadoop.hbase.types.Struct;
import org.apache.hadoop.hbase.types.StructBuilder;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.SimplePositionedByteRange;
import org.apache.hadoop.io.IntWritable;

import java.io.IOException;
import java.util.Map;

import static dev.tsuyo.hbaseiid.ch7.blog.BlogService.D_BYTES;

public class IndexMapper extends TableMapper<ImmutableBytesWritable, IntWritable> {
  private final Struct blogRowSchema;

  public IndexMapper() {
    blogRowSchema = new StructBuilder()
        .add(new RawInteger()) // hash of user ID
        .add(new RawLong()) // user ID
        .toStruct();
  }

  @Override
  protected void map(ImmutableBytesWritable key, Result value, Context context)
      throws IOException, InterruptedException {
    for (Map.Entry<byte[], byte[]> columnEntry : value.getFamilyMap(D_BYTES).entrySet()) {
      long articleId = Long.MAX_VALUE - Bytes.toLong(columnEntry.getKey());
      byte[] val = columnEntry.getValue();

      long userId = (long) blogRowSchema.decode(new SimplePositionedByteRange(key.get()), 1);

      Article article = BlogService.deserializeArticle(val);
      String[] splitContent = article.getContent().split(" ");
      for (int i = 0; i < splitContent.length; i++) {
        ImmutableBytesWritable immutableBytesWritable = new ImmutableBytesWritable(
            Bytes.add(Bytes.toBytes(userId), Bytes.toBytes(articleId), Bytes.toBytes(splitContent[i]))
        );
        context.write(immutableBytesWritable, new IntWritable(i));
      }
    }
  }
}
