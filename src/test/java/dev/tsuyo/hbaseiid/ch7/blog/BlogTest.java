package dev.tsuyo.hbaseiid.ch7.blog;

import dev.tsuyo.hbaseiid.ColumnFamilyOption;
import dev.tsuyo.hbaseiid.Utils;
import org.apache.hadoop.hbase.client.Connection;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.stream.IntStream;

import static dev.tsuyo.hbaseiid.ch7.blog.BlogService.*;
import static org.junit.jupiter.api.Assertions.*;


public class BlogTest {
  private static final Logger logger = LoggerFactory.getLogger(BlogTest.class);

  private static Connection connection;
  // private static BlogService blogService;
  private BlogService blogService;

  @BeforeAll
  static void setup() throws IOException {
    connection = Utils.getConnection();
    // blogService = new BlogServiceImpl();
  }

  @BeforeEach
  void initTable() throws IOException {
    Utils.initTable(connection, NS_STR, BLOG_STR, Arrays.asList(
        ColumnFamilyOption.from(D_STR)
            .addParameter("BLOOMFILTER", "ROW")
            .build(),
        ColumnFamilyOption.from(S_STR)
            .addParameter("BLOOMFILTER", "ROW")
            .build()
        )
    );

    blogService = new BlogServiceImpl();
  }

  @AfterAll
  static void tearDown() throws IOException {
    connection.close();
  }

  private void postArticle(long userId, String title, String content, int categoryId) {
    try {
      blogService.postArticle(userId, title, content, categoryId);
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  @Test
  public void testGetArticle() throws IOException {
    blogService.postArticle(1L, "title 1", "content 1", 1);

    Article article = blogService.getArticle(1L, 0L);
    assertEquals(0L, article.getArticleId());
    assertEquals(1, article.getCategoryId());
    assertEquals("content 1", article.getContent());
    assertEquals("title 1", article.getTitle());
    assertEquals(1L, article.getUserId());
  }

  @Test
  public void testUpdateArticle() throws IOException {
    blogService.postArticle(1L, "title 1", "content 1", 1);
    blogService.updateArticle(1L, 0L, "new title 1", "new content 1", 11);

    Article article = blogService.getArticle(1L, 0L);
    assertEquals(0L, article.getArticleId());
    assertEquals(11, article.getCategoryId());
    assertEquals("new content 1", article.getContent());
    assertEquals("new title 1", article.getTitle());
    assertEquals(1L, article.getUserId());
  }

  @Test
  public void testDeleteArticle() throws IOException {
    blogService.postArticle(1L, "title 1", "content 1", 1);
    blogService.deleteArticle(1L, 0L);
    Article article = blogService.getArticle(1L, 0L);
    assertNull(article);
  }

  @Test
  public void testGetArticles() throws IOException {
    IntStream.range(0, 10).forEach(i -> postArticle(1L, "u1 title " + i, "u1 content " + i, i));
    IntStream.range(0, 10).forEach(i -> postArticle(2L, "u2 title " + i, "u2 content " + i, i));

    List<Article> articles = blogService.getArticles(1L, 5L, 3);
    assertEquals(articles.size(), 3);
    int i = 4;
    for (Article article : articles) {
      assertEquals(i, article.getArticleId());
      assertEquals(i, article.getCategoryId());
      assertEquals("u1 content " + i, article.getContent());
      assertEquals("u1 title " + i, article.getTitle());
      assertEquals(1L, article.getUserId());
      i--;
    }
  }

  @Test
  public void testGetArticlesByCategoryId() throws IOException {
    // odd articldId has category 0 and even articleId has 1
    IntStream.range(0, 10).forEach(i -> postArticle(1L, "u1 title " + i, "u1 content " + i, i % 2));
    IntStream.range(0, 10).forEach(i -> postArticle(2L, "u2 title " + i, "u2 content " + i, i % 2));

    List<Article> articles = blogService.getArticles(1L, 0, 9L, 3);
    assertEquals(articles.size(), 3);
    int i = 8;
    // secondary index only has title & publishAt in HBase (+ add articleId, categoryId, userId in BlogServiceImpl)
    for (Article article : articles) {
      assertEquals(i, article.getArticleId());
      assertEquals(0, article.getCategoryId());
      assertEquals("u1 title " + i, article.getTitle());
      assertEquals(1L, article.getUserId());
      i -= 2;
    }
  }

}
