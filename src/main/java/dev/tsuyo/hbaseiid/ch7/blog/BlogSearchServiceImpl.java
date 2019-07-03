package dev.tsuyo.hbaseiid.ch7.blog;

import dev.tsuyo.hbaseiid.Utils;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.types.RawInteger;
import org.apache.hadoop.hbase.types.RawLong;
import org.apache.hadoop.hbase.types.Struct;
import org.apache.hadoop.hbase.types.StructBuilder;
import org.apache.hadoop.hbase.util.ByteArrayHashKey;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Hash;
import org.apache.hadoop.hbase.util.SimplePositionedMutableByteRange;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import static dev.tsuyo.hbaseiid.ch7.blog.BlogService.D_BYTES;
import static dev.tsuyo.hbaseiid.ch7.blog.BlogService.NS_STR;

public class BlogSearchServiceImpl implements BlogSearchService {

  private final Connection connection;
  private final Hash hash;
  private final Struct blogSearchIndexRowSchema; // for blogsearch table row

  private final AtomicLong idGenerator = new AtomicLong();

  public BlogSearchServiceImpl() throws IOException {
    connection = Utils.getConnection();
    hash = Hash.getInstance(Hash.MURMUR_HASH3);
    blogSearchIndexRowSchema = new StructBuilder()
        .add(new RawInteger()) // hash of user ID
        .add(new RawLong()) // user ID
        .toStruct();
  }

  @Override
  public List<SearchResult> search(long userId, String word) throws IOException {
    byte[] row = createBlogSearchIndexRow(userId);

    Get get = new Get(row)
        .addColumn(D_BYTES, Bytes.toBytes(word))
        .readAllVersions();

    try (Table table = connection.getTable(TableName.valueOf(NS_STR, BLOGSEARCH_STR))) {
      List<SearchResult> searchResults = new ArrayList<>();

      Result result = table.get(get);
      if (result.isEmpty()) {
        return searchResults;
      }

      for (Map.Entry<Long, byte[]> entry : result.getMap().get(D_BYTES).get(Bytes.toBytes(word)).entrySet()) {
        SearchResult searchResult = new SearchResult();
        searchResult.setArticleId(entry.getKey());
        searchResult.setOffsets(Utils.deserializeAsSortedSetInteger(entry.getValue()));
        searchResults.add(searchResult);
      }
      return searchResults;
    }
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
