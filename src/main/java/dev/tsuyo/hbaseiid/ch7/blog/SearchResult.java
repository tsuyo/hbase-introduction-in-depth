package dev.tsuyo.hbaseiid.ch7.blog;

import java.util.List;
import java.util.SortedSet;

public class SearchResult {
  private long articleId;

  // list of offsets which matches a search word for highlighting
  // TODO: PR
  // private List<Integer> offsets;
  private SortedSet<Integer> offsets;

  public long getArticleId() {
    return articleId;
  }

  public void setArticleId(long articleId) {
    this.articleId = articleId;
  }

  public SortedSet<Integer> getOffsets() {
    return offsets;
  }

  public void setOffsets(SortedSet<Integer> offsets) {
    this.offsets = offsets;
  }
}
