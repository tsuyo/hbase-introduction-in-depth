package dev.tsuyo.hbaseiid.ch7.documentversion;

import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.List;

public interface DocumentVersionManager {
  static final String NS_STR = "ns";
  static final String DOCUMENT_STR = "document";
  static final String D_STR = "d";
  static final byte[] D_BYTES = Bytes.toBytes(D_STR);
  static final byte[] VER_BYTES = Bytes.toBytes("ver");
  static final byte[] TEXT_BYTES = Bytes.toBytes("text");
  static final byte[] TITLE_BYTES = Bytes.toBytes("title");

  void save(String documentId, String title, String text) throws IOException;

  // get all versions. return the latest one first
  List<Long> listVersions(String documentId) throws IOException;

  Document getLatest(String documentId) throws IOException;

  Document get(String documentId, long version) throws IOException;
}
