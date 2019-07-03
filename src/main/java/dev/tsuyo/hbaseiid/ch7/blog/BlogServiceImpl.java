package dev.tsuyo.hbaseiid.ch7.blog;

import dev.tsuyo.hbaseiid.Utils;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.ColumnPaginationFilter;
import org.apache.hadoop.hbase.types.RawInteger;
import org.apache.hadoop.hbase.types.RawLong;
import org.apache.hadoop.hbase.types.Struct;
import org.apache.hadoop.hbase.types.StructBuilder;
import org.apache.hadoop.hbase.util.*;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.SerializationConfig;
import org.codehaus.jackson.map.annotate.JsonSerialize;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

public class BlogServiceImpl implements BlogService {

  private final Connection connection;
  private final Hash hash;
  private final Struct blogRowSchema; // for blog table row
  private final Struct secondaryIndexQualifierSchema; // for secondary index column

  private final AtomicLong idGenerator = new AtomicLong();

  public BlogServiceImpl() throws IOException {
    connection = Utils.getConnection();
    hash = Hash.getInstance(Hash.MURMUR_HASH3);
    blogRowSchema = new StructBuilder()
        .add(new RawInteger()) // hash of user ID
        .add(new RawLong()) // user ID
        .toStruct();
    secondaryIndexQualifierSchema = new StructBuilder()
        .add(new RawInteger()) // category ID
        .add(new RawLong()) // user ID
        .toStruct();
  }

  // TODO: articleId should be returned because it's auto-generated inside this function
  @Override
  public void postArticle(long userId, String title, String content, int categoryId) throws IOException {
    long postAt = System.currentTimeMillis();
    long updateAt = postAt;
    String categoryName = getCategoryName(categoryId);
    long articleId = createArticleId();

    byte[] row = createBlogRow(userId);
    Put put = new Put(row)
        .addColumn(D_BYTES, Bytes.toBytes(Long.MAX_VALUE - articleId), serialize(title, content, categoryId, categoryName, postAt, updateAt))
        .addColumn(S_BYTES, createSecondaryIndexQualifier(categoryId, articleId), BlogService.serializeArticle(title, postAt));

    try (Table table = connection.getTable(TableName.valueOf(NS_STR, BLOG_STR))) {
      table.put(put);
    }
  }

  @Override
  public void updateArticle(long userId, long articleId, String newTitle, String newContent, int newCategoryId) throws IOException {
    byte[] row = createBlogRow(userId);

    try (Table table = connection.getTable(TableName.valueOf(NS_STR, BLOG_STR))) {
      Get get = new Get(row)
          .addColumn(D_BYTES, Bytes.toBytes(Long.MAX_VALUE - articleId));
      Result result = table.get(get);
      if (result.isEmpty()) {
        return; // not found such an article
      }
      Article article = BlogService.deserializeArticle(result.value());
      String newCategoryName = article.getCategoryId() == newCategoryId ? article.getCategoryName() : getCategoryName(newCategoryId);
      long updateAt = System.currentTimeMillis();

      RowMutations rowMutations = new RowMutations(row);

      // RowMutations.add(Put) is deprecated
      Mutation put = new Put(row)
          .addColumn(D_BYTES, Bytes.toBytes(Long.MAX_VALUE - articleId),
              serialize(newTitle, newContent, newCategoryId, newCategoryName, article.getPostAt(), updateAt))
          .addColumn(S_BYTES, createSecondaryIndexQualifier(newCategoryId, articleId),
              BlogService.serializeArticle(newTitle, article.getPostAt()));

      rowMutations.add(put);

      if (article.getCategoryId() != newCategoryId) {
        // RowMutations.add(Delete) is deprecated
        //
        // Delete all obsolete secondary index (oldcategoryId + (reverse-articleId)
        //
        // (From JavaDoc)
        // If no further operations are done, this will delete all columns in all families of the specified row
        // with a timestamp less than or equal to the specified timestamp.
        Mutation delete = new Delete(row, updateAt)
            .addColumn(S_BYTES, createSecondaryIndexQualifier(article.getCategoryId(), articleId));
        rowMutations.add(delete);
      }

      table.mutateRow(rowMutations);
    }

  }

  @Override
  public void deleteArticle(long userId, long articleId) throws IOException {
    byte[] row = createBlogRow(userId);

    try (Table table = connection.getTable(TableName.valueOf(NS_STR, BLOG_STR))) {
      Get get = new Get(row).addColumn(D_BYTES, Bytes.toBytes(Long.MAX_VALUE - articleId));
      Result result = table.get(get);
      if (result.isEmpty()) {
        return; // no such articleId
      }
      Article article = BlogService.deserializeArticle(result.value());
      Delete delete = new Delete(row)
          .addColumn(D_BYTES, Bytes.toBytes(Long.MAX_VALUE - articleId))
          .addColumn(S_BYTES, createSecondaryIndexQualifier(article.getCategoryId(), articleId));
      table.delete(delete);
    }
  }

  @Override
  public Article getArticle(long userId, long articleId) throws IOException {
    byte[] row = createBlogRow(userId);
    Get get = new Get(row).addColumn(D_BYTES, Bytes.toBytes(Long.MAX_VALUE - articleId));

    try (Table table = connection.getTable(TableName.valueOf(NS_STR, BLOG_STR))) {
      Result result = table.get(get);
      if (result.isEmpty()) {
        return null; // no such articleId
      }
      Article article = BlogService.deserializeArticle(result.getValue(D_BYTES, Bytes.toBytes(Long.MAX_VALUE - articleId)));
      article.setUserId(userId);
      article.setArticleId(articleId);
      return article;
    }
  }

  @Override
  public List<Article> getArticles(long userId, Long lastArticleId, int length) throws IOException {
    byte[] row = createBlogRow(userId);
    Get get = new Get(row).addFamily(D_BYTES);

    if (lastArticleId == null) {
      get.setFilter(new ColumnPaginationFilter(length, 0));
    } else {
      get.setFilter(new ColumnPaginationFilter(length, Bytes.toBytes(Long.MAX_VALUE - lastArticleId + 1)));
    }

    try (Table table = connection.getTable(TableName.valueOf(NS_STR, BLOG_STR))) {
      Result result = table.get(get);

      List<Article> ret = new ArrayList<>();
      for (Map.Entry<byte[], byte[]> entry : result.getFamilyMap(D_BYTES).entrySet()) {
        long articleId = Long.MAX_VALUE - Bytes.toLong(entry.getKey());
        byte[] value = entry.getValue();
        Article article = BlogService.deserializeArticle(value);
        article.setUserId(userId);
        article.setArticleId(articleId);
        ret.add(article);
      }
      return ret;
    }
  }

  @Override
  public List<Article> getArticles(long userId, int categoryId, Long lastArticleId, int length) throws IOException {
    byte[] row = createBlogRow(userId);
    Get get = new Get(row).addFamily(S_BYTES);

    if (lastArticleId == null) {
      get.setFilter(new ColumnPaginationFilter(length, Bytes.toBytes(categoryId)));
    } else {
      get.setFilter(new ColumnPaginationFilter(length, Utils.incrementBytes(createSecondaryIndexQualifier(categoryId, lastArticleId))));
    }

    try (Table table = connection.getTable(TableName.valueOf(NS_STR, BLOG_STR))) {
      Result result = table.get(get);

      List<Article> ret = new ArrayList<>();
      for (Map.Entry<byte[], byte[]> entry : result.getFamilyMap(S_BYTES).entrySet()) {
        long reversedArticleId = (Long) secondaryIndexQualifierSchema.decode(new SimplePositionedByteRange(entry.getKey()), 1);
        byte[] value = entry.getValue();
        Article article = BlogService.deserializeArticle(value);
        article.setUserId(userId);
        article.setArticleId(Long.MAX_VALUE - reversedArticleId);
        article.setCategoryId(categoryId);
        ret.add(article);
      }
      return ret;
    }
  }

  private String getCategoryName(int categoryId) {
    return Integer.toString(categoryId);
  }

  private long createArticleId() {
    return idGenerator.getAndIncrement();
  }

  private byte[] createBlogRow(long userId) {
    byte[] userIdBytes = Bytes.toBytes(userId);
    Object[] values = new Object[]{
        hash.hash(new ByteArrayHashKey(userIdBytes, 0, userIdBytes.length), 0),
        userId
    };
    SimplePositionedMutableByteRange positionedByteRange =
        new SimplePositionedMutableByteRange(blogRowSchema.encodedLength(values));
    blogRowSchema.encode(positionedByteRange, values);
    return positionedByteRange.getBytes();
  }

  private byte[] serialize(String title, String content, int categoryId, String categoryName, long postAt, long updateAt) {
    Article article = new Article();
    article.setTitle(title);
    article.setContent(content);
    article.setCategoryId(categoryId);
    article.setCategoryName(categoryName);
    article.setPostAt(postAt);
    article.setUpdateAt(updateAt);

    ObjectMapper objectMapper = new ObjectMapper();
    // SerializationConfig.setSerializationInclusion() is deprecated
    SerializationConfig config = objectMapper.getSerializationConfig()
        .withSerializationInclusion(JsonSerialize.Inclusion.NON_NULL);

    try {
      return objectMapper.writeValueAsBytes(article);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private byte[] createSecondaryIndexQualifier(int categoryId, long articleId) {
    Object[] values = new Object[]{
        categoryId,
        Long.MAX_VALUE - articleId
    };
    SimplePositionedMutableByteRange positionedByteRange =
        new SimplePositionedMutableByteRange(secondaryIndexQualifierSchema.encodedLength(values));
    secondaryIndexQualifierSchema.encode(positionedByteRange, values);
    return positionedByteRange.getBytes();
  }


}
