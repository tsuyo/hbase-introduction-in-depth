package dev.tsuyo.hbaseiid.ch7.blog;

import java.io.IOException;
import java.util.List;

public interface BlogSearchService {
  static final String BLOGSEARCH_STR = "blogsearch";

  List<SearchResult> search(long userId, String word) throws IOException;
}
