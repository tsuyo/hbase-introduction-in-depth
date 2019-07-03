package dev.tsuyo.hbaseiid.ch7.documentversion;

import dev.tsuyo.hbaseiid.Utils;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.types.*;
import org.apache.hadoop.hbase.util.*;

import java.io.IOException;
import java.util.*;

public class DocumentVersionManagerImpl implements DocumentVersionManager {

  private final Connection connection;
  private final Hash hash;
  private final Struct documentRowSchema;

  public DocumentVersionManagerImpl() throws IOException {
    connection = Utils.getConnection();
    hash = Hash.getInstance(Hash.MURMUR_HASH3);
    documentRowSchema = new StructBuilder()
        .add(new RawInteger()) // hash of document ID
        .add(RawString.ASCENDING) // document ID
        .toStruct();
  }

  @Override
  public void save(String documentId, String title, String text) throws IOException {
    byte[] row = createDocumentRow(documentId);

    try (Table table = connection.getTable(TableName.valueOf(NS_STR, DOCUMENT_STR))) {
      while (true) {
        Get get = new Get(row).addColumn(D_BYTES, VER_BYTES);
        Result verResult = table.get(get);
        Long version = null;
        byte[] oldVersionBytes = null;
        if (verResult.isEmpty()) {
          oldVersionBytes = HConstants.EMPTY_BYTE_ARRAY;
          version = 1L;
        } else {
          oldVersionBytes = verResult.getValue(D_BYTES, VER_BYTES);
          long oldVersion = Bytes.toLong(oldVersionBytes);
          version = oldVersion + 1;
        }

        // Put.add() deprecated
        Put put = new Put(row, version)
            .addColumn(D_BYTES, VER_BYTES, Bytes.toBytes(version))
            .addColumn(D_BYTES, TEXT_BYTES, Bytes.toBytes(text))
            .addColumn(D_BYTES, TITLE_BYTES, Bytes.toBytes(title));

        // Table.checkAndPut() deprecated
        boolean success = table.checkAndMutate(row, D_BYTES).qualifier(VER_BYTES).ifEquals(oldVersionBytes).thenPut(put);
        if (success) {
          return;
        }
      }
    }

  }

  @Override
  public List<Long> listVersions(String documentId) throws IOException {
    byte[] row = createDocumentRow(documentId);
    // Get.setMaxVersions() deprecated
    Get get = new Get(row).addColumn(D_BYTES, VER_BYTES)
        .readAllVersions();

    List<Long> versions = new ArrayList<>();
    try (Table table = connection.getTable(TableName.valueOf(NS_STR, DOCUMENT_STR))) {
      Result result = table.get(get);
      for (Long version : result.getMap().get(D_BYTES).get(VER_BYTES).keySet()) {
        versions.add(version);
      }
      return versions;
    }
  }

  @Override
  public Document getLatest(String documentId) throws IOException {
    byte[] row = createDocumentRow(documentId);

    try (Table table = connection.getTable(TableName.valueOf(NS_STR, DOCUMENT_STR))) {
      Get get = new Get(row)
          .addColumn(D_BYTES, VER_BYTES)
          .addColumn(D_BYTES, TITLE_BYTES)
          .addColumn(D_BYTES, TEXT_BYTES);

      Result result = table.get(get);
      if (result.isEmpty()) {
        return null;
      }
      Document document = new Document();
      document.setDocumentId(documentId);
      document.setVersion(Bytes.toLong(result.getValue(D_BYTES, VER_BYTES)));
      document.setTitle(Bytes.toString(result.getValue(D_BYTES, TITLE_BYTES)));
      document.setText(Bytes.toString(result.getValue(D_BYTES, TEXT_BYTES)));

      return document;
    }
  }

  @Override
  public Document get(String documentId, long version) throws IOException {
    byte[] row = createDocumentRow(documentId);

    try (Table table = connection.getTable(TableName.valueOf(NS_STR, DOCUMENT_STR))) {
      Get get = new Get(row)
          .addColumn(D_BYTES, TITLE_BYTES)
          .addColumn(D_BYTES, TEXT_BYTES)
          .setTimestamp(version);

      Result result = table.get(get);
      if (result.isEmpty()) {
        return null;
      }
      Document document = new Document();
      document.setDocumentId(documentId);
      document.setVersion(version);
      document.setTitle(Bytes.toString(result.getValue(D_BYTES, TITLE_BYTES)));
      document.setText(Bytes.toString(result.getValue(D_BYTES, TEXT_BYTES)));

      return document;
    }
  }

  private byte[] createDocumentRow(String documentId) {
    byte[] documentIdBytes = Bytes.toBytes(documentId);
    Object[] values = new Object[]{
        hash.hash(new ByteArrayHashKey(documentIdBytes, 0, documentIdBytes.length), 0), // hash API changed
        documentId
    };
    SimplePositionedMutableByteRange positionedByteRange =
        new SimplePositionedMutableByteRange(documentRowSchema.encodedLength(values));
    documentRowSchema.encode(positionedByteRange, values);
    return positionedByteRange.getBytes();
  }
}
