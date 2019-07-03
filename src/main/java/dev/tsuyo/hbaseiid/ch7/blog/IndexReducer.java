package dev.tsuyo.hbaseiid.ch7.blog;

import dev.tsuyo.hbaseiid.Utils;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.types.RawInteger;
import org.apache.hadoop.hbase.types.RawLong;
import org.apache.hadoop.hbase.types.Struct;
import org.apache.hadoop.hbase.types.StructBuilder;
import org.apache.hadoop.hbase.util.ByteArrayHashKey;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Hash;
import org.apache.hadoop.hbase.util.SimplePositionedMutableByteRange;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.SortedSet;
import java.util.TreeSet;

import static dev.tsuyo.hbaseiid.ch7.blog.BlogService.D_BYTES;

public class IndexReducer extends TableReducer<ImmutableBytesWritable, IntWritable, NullWritable> {
  private final Hash hash;
  private final Struct blogSearchIndexRowSchema; // for blog search table row

  public IndexReducer() {
    hash = Hash.getInstance(Hash.MURMUR_HASH3);
    blogSearchIndexRowSchema = new StructBuilder()
        .add(new RawInteger()) // hash of user ID
        .add(new RawLong()) // user ID
        .toStruct();
  }

  @Override
  protected void reduce(ImmutableBytesWritable key, Iterable<IntWritable> values, Context context)
      throws IOException, InterruptedException {
    ByteBuffer buffer = ByteBuffer.wrap(key.get());
    long userId = buffer.getLong();
    long articleId = buffer.getLong();
    byte[] word = new byte[buffer.capacity() - buffer.position()];
    buffer.get(word);

    SortedSet<Integer> offsets = new TreeSet<>();
    for (IntWritable value : values) {
      offsets.add(value.get());
    }

    byte[] row = createBlogSearchIndexRow(userId);
    Put put = new Put(row)
        .addColumn(D_BYTES, word, articleId, Utils.serialize(offsets));
    context.write(NullWritable.get(), put);
  }

  private byte[] createBlogSearchIndexRow(long userId) {
    byte[] userIdBytes = Bytes.toBytes(userId);
    Object[] values = new Object[]{
        hash.hash(new ByteArrayHashKey(userIdBytes, 0, userIdBytes.length), 0), // hash API changed
        userId
    };
    SimplePositionedMutableByteRange positionedByteRange =
        new SimplePositionedMutableByteRange(blogSearchIndexRowSchema.encodedLength(values));
    blogSearchIndexRowSchema.encode(positionedByteRange, values);
    return positionedByteRange.getBytes();
  }

}
