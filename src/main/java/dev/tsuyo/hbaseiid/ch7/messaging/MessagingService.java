package dev.tsuyo.hbaseiid.ch7.messaging;

import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.List;

public interface MessagingService {
  public static final String M_NS_STR = "ns";
  public static final String M_TABLE_STR = "message";
  public static final byte[] M_FAM = Bytes.toBytes("m");
  public static final byte[] M_MESSAGE_ID = Bytes.toBytes("messageId");
  public static final byte[] M_USER_ID = Bytes.toBytes("userId");
  public static final byte[] M_BODY = Bytes.toBytes("body");

  // send a message
  void sendMessage(long roomId, long userId, String body) throws IOException;

  // get initial messages when joining a room
  List<Message> getInitialMessages(long roomId, List<Long> blockUsers) throws IOException;

  // get past messages
  List<Message> getOldMessages(long roomId, Message oldestReceivedMessage, List<Long> blockUsers) throws IOException;

  // get new messages
  List<Message> getNewMessages(long roomId, Message latestReceivedMessage, List<Long> blockUsers) throws IOException;
}
