package dev.tsuyo.hbaseiid.ch7.documentversion;

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

import static dev.tsuyo.hbaseiid.ch7.documentversion.DocumentVersionManager.*;
import static org.junit.jupiter.api.Assertions.assertEquals;


public class DocumentVersionTest {
  private static final Logger logger = LoggerFactory.getLogger(DocumentVersionTest.class);

  private static Connection connection;
  private static DocumentVersionManager documentVersionManager;

  @BeforeAll
  static void setup() throws IOException {
    connection = Utils.getConnection();
    documentVersionManager = new DocumentVersionManagerImpl();
  }

  @BeforeEach
  void initTable() throws IOException {
    Utils.initTable(connection, NS_STR, DOCUMENT_STR, Arrays.asList(
        ColumnFamilyOption.from(D_STR)
            .addParameter("VERSIONS", "100")
            .addParameter("BLOOMFILTER", "ROW")
            .build()
        )
    );
  }

  @AfterAll
  static void tearDown() throws IOException {
    connection.close();
  }

  private void save(String documentId, String title, String text) {
    try {
      documentVersionManager.save(documentId, title, text);
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  @Test
  public void testListVersions() throws IOException {
    // insert "title 1" - "title 5"
    IntStream.range(1, 6).forEach(i -> save("doc", "title " + i, "text " + i));
    List<Long> ids = documentVersionManager.listVersions("doc");
    assertEquals(5, ids.size());
  }

  @Test
  public void testGetLatest() throws IOException {
    IntStream.range(1, 6).forEach(i -> save("doc", "title " + i, "text " + i));
    Document document = documentVersionManager.getLatest("doc");
    assertEquals("doc", document.getDocumentId());
    assertEquals("text 5", document.getText());
    assertEquals("title 5", document.getTitle());
    assertEquals(5L, document.getVersion());
  }

  @Test
  public void testGet() throws IOException {
    IntStream.range(1, 6).forEach(i -> save("doc", "title " + i, "text " + i));
    Document document = documentVersionManager.get("doc", 3L);
    assertEquals("doc", document.getDocumentId());
    assertEquals("text 3", document.getText());
    assertEquals("title 3", document.getTitle());
    assertEquals(3L, document.getVersion());
  }

}
