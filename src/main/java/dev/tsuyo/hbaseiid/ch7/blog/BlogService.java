package dev.tsuyo.hbaseiid.ch7.blog;

import org.apache.hadoop.hbase.util.Bytes;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.SerializationConfig;
import org.codehaus.jackson.map.annotate.JsonSerialize;

import java.io.IOException;
import java.util.List;

public interface BlogService {
  String NS_STR = "ns";
  String BLOG_STR = "blog";
  String D_STR = "d";
  String S_STR = "s";
  byte[] D_BYTES = Bytes.toBytes(D_STR);
  byte[] S_BYTES = Bytes.toBytes(S_STR);

  static byte[] serializeArticle(String title, long postAt) {
    Article article = new Article();
    article.setTitle(title);
    article.setPostAt(postAt);

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

  static Article deserializeArticle(byte[] bytes) {
    ObjectMapper objectMapper = new ObjectMapper();
    try {
      return objectMapper.readValue(bytes, Article.class);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  void postArticle(long userId, String title, String content, int categoryId) throws IOException;

  void updateArticle(long userId, long articleId, String newTitle, String newContent, int newCategoryId) throws IOException;

  void deleteArticle(long userId, long articleId) throws IOException;

  Article getArticle(long userId, long articleId) throws IOException;

  // get the *length* articles by the latest order
  // lastArticledId is for paging
  List<Article> getArticles(long userId, Long lastArticleId, int length) throws IOException;

  // get the *length* articles in a *categoryId* by the latest order
  List<Article> getArticles(long userId, int categoryId, Long lastArticleId, int length) throws IOException;

}
