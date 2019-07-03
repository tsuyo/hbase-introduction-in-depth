package dev.tsuyo.hbaseiid.ch7.messaging;

import dev.tsuyo.hbaseiid.Utils;
import org.apache.hadoop.hbase.CompareOperator;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.types.*;
import org.apache.hadoop.hbase.util.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

public class MessagingServiceImpl implements MessagingService {
  private final Connection connection;
  private final Hash hash;
  private final Struct messageRowSchema;

  public MessagingServiceImpl() throws IOException {
    connection = Utils.getConnection();
    hash = Hash.getInstance(Hash.MURMUR_HASH3);
    messageRowSchema = new StructBuilder()
        .add(new RawInteger())
        .add(new RawLong())
        .add(new RawLong())
        .add(RawString.ASCENDING)
        .toStruct();
  }

  @Override
  public void sendMessage(long roomId, long userId, String body) throws IOException {
    long postAt = System.currentTimeMillis();
    String messageId = UUID.randomUUID().toString();
    byte[] row = createMessageRow(roomId, postAt, messageId);

    Put put = new Put(row)
        .addColumn(M_FAM, M_MESSAGE_ID, Bytes.toBytes(messageId))
        .addColumn(M_FAM, M_USER_ID, Bytes.toBytes(userId))
        .addColumn(M_FAM, M_BODY, Bytes.toBytes(body));

    try (Table table = connection.getTable(TableName.valueOf(M_NS_STR, M_TABLE_STR))) {
      table.put(put);
    }
  }

  @Override
  public List<Message> getInitialMessages(long roomId, List<Long> blockUsers) throws IOException {
    byte[] startRow = createMessageScanRow(roomId);
    byte[] stopRow = Utils.incrementBytes(createMessageScanRow(roomId));

    return getMessages(startRow, stopRow, blockUsers);
  }

  @Override
  public List<Message> getNewMessages(long roomId, Message latestReceivedMessage, List<Long> blockUsers) throws IOException {
    byte[] startRow = createMessageScanRow(roomId);
    byte[] stopRow = createMessageRow(roomId, latestReceivedMessage.getPostAt(), latestReceivedMessage.getMessageId());

    return getMessages(startRow, stopRow, blockUsers);
  }

  @Override
  public List<Message> getOldMessages(long roomId, Message oldestReceivedMessage, List<Long> blockUsers) throws IOException {
    byte[] startRow = Utils.incrementBytes(createMessageRow(roomId, oldestReceivedMessage.getPostAt(), oldestReceivedMessage.getMessageId()));
    byte[] stopRow = Utils.incrementBytes(createMessageScanRow(roomId));

    return getMessages(startRow, stopRow, blockUsers);
  }

  private byte[] createMessageRow(long roomId, long postAt, String messageId) {
    byte[] roomIdBytes = Bytes.toBytes(roomId);
    Object[] values = new Object[]{
        hash.hash(new ByteArrayHashKey(roomIdBytes, 0, roomIdBytes.length), 0),
        roomId,
        Long.MAX_VALUE - postAt,
        messageId
    };

    SimplePositionedMutableByteRange positionedByteRange =
        new SimplePositionedMutableByteRange(messageRowSchema.encodedLength(values));
    messageRowSchema.encode(positionedByteRange, values);

    return positionedByteRange.getBytes();
  }

  private Message convertToMessage(Result result) {
    Message message = new Message();
    message.setUserId(Bytes.toLong(result.getValue(M_FAM, M_USER_ID)));
    message.setBody(Bytes.toString(result.getValue(M_FAM, M_BODY)));
    message.setMessageId(Bytes.toString(result.getValue(M_FAM, M_MESSAGE_ID)));
    // TODO: PR #3 (DONE)
    message.setPostAt(Long.MAX_VALUE - (long) messageRowSchema.decode(new SimplePositionedByteRange(result.getRow()), 2));

    return message;
  }

  private byte[] createMessageScanRow(long roomId) {
    byte[] roomIdBytes = Bytes.toBytes(roomId);
    Object[] values = new Object[]{
        hash.hash(new ByteArrayHashKey(roomIdBytes, 0, roomIdBytes.length), 0),
        roomId
    };
    SimplePositionedMutableByteRange positionedByteRange =
        new SimplePositionedMutableByteRange(messageRowSchema.encodedLength(values));
    messageRowSchema.encode(positionedByteRange, values);
    return positionedByteRange.getBytes();
  }

  private List<Message> getMessages(byte[] startRow, byte[] stopRow, List<Long> blockUsers) throws IOException {
    Scan scan = new Scan().withStartRow(startRow).withStopRow(stopRow);

    FilterList filterList = new FilterList(FilterList.Operator.MUST_PASS_ALL);

    if (blockUsers != null) {
      for (Long userId : blockUsers) {
        SingleColumnValueFilter userFilter
            = new SingleColumnValueFilter(M_FAM, M_USER_ID, CompareOperator.NOT_EQUAL, Bytes.toBytes(userId));
        filterList.addFilter(userFilter);
      }
    }

    scan.setFilter(filterList);

    List<Message> messages = new ArrayList<>();
    try (Table table = connection.getTable(TableName.valueOf(M_NS_STR, M_TABLE_STR))) {
      ResultScanner scanner = table.getScanner(scan);
      int count = 0;
      for (Result result : scanner) {
        messages.add(convertToMessage(result));
        count++;
        if (count >= 50) break;
      }
    }

    return messages;
  }

}
