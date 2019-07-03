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
import java.util.SortedSet;
import java.util.TreeSet;

import static dev.tsuyo.hbaseiid.ch7.blog.BlogSearchService.BLOGSEARCH_STR;
import static dev.tsuyo.hbaseiid.ch7.blog.BlogService.*;
import static org.junit.jupiter.api.Assertions.assertEquals;


public class BlogSearchTest {
  private static final Logger logger = LoggerFactory.getLogger(BlogSearchTest.class);

  private static Connection connection;
  private static BlogService blogService;
  private static BlogSearchService blogSearchService;
  private static Indexer indexer;

  @BeforeAll
  static void setup() throws IOException {
    connection = Utils.getConnection();
    blogService = new BlogServiceImpl();
    blogSearchService = new BlogSearchServiceImpl();
    indexer = new Indexer();
  }

  @BeforeEach
  void initTable() throws IOException {
    // create a blog table
    Utils.initTable(connection, NS_STR, BLOG_STR, Arrays.asList(
        ColumnFamilyOption.from(D_STR)
            .addParameter("BLOOMFILTER", "ROW")
            .build(),
        ColumnFamilyOption.from(S_STR)
            .addParameter("BLOOMFILTER", "ROW")
            .build()
        )
    );

    // create a blogsearch table
    Utils.initTable(connection, NS_STR, BLOGSEARCH_STR, Arrays.asList(
        ColumnFamilyOption.from(D_STR)
            .addParameter("VERSIONS", Integer.MAX_VALUE + "")
            .addParameter("BLOOMFILTER", "ROWCOL")
            .build()
        )
    );
  }

  @AfterAll
  static void tearDown() throws IOException {
    connection.close();
  }

  @Test
  public void testSearch() throws Exception {
    // Insert test data
    blogService.postArticle(1L, "title 1", "aa aa bb", 1);
    blogService.postArticle(1L, "title 2", "bb bb cc", 2);
    blogService.postArticle(1L, "title 3", "cc cc dd", 3);
    blogService.postArticle(2L, "title 4", "dd dd ee", 4);
    blogService.postArticle(3L, "title 5", "ee ff gg", 5);

    // Run an indexer (MapReduce) job
    indexer.startIndex();

    // search
    // you can retrieve all the index with > scan 'ns:blogsearch', {VERSIONS=>3}

    List<SearchResult> searchResults = blogSearchService.search(1L, "bb");

    assertEquals(2, searchResults.size());

    // we assume the article ID generator generates 0, 1, 2, .. in order
    // and search() returns the latest timestamp (articleID) first (in this case, 1 -> 0)
    assertEquals(1L, searchResults.get(0).getArticleId());
    SortedSet<Integer> expectedOffsets = new TreeSet<>();
    expectedOffsets.add(0);
    expectedOffsets.add(1);
    assertEquals(expectedOffsets, searchResults.get(0).getOffsets());

    assertEquals(0L, searchResults.get(1).getArticleId());
    expectedOffsets = new TreeSet<Integer>();
    expectedOffsets.add(2);
    assertEquals(expectedOffsets, searchResults.get(1).getOffsets());
  }

}
