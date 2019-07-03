package dev.tsuyo.hbaseiid.ch7.messaging;

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

import static org.junit.jupiter.api.Assertions.*;


public class MessagingTest {
  private static final Logger logger = LoggerFactory.getLogger(MessagingTest.class);

  private static Connection connection;
  private static MessagingService messagingService;

  @BeforeAll
  static void setup() throws IOException {
    connection = Utils.getConnection();
    messagingService = new MessagingServiceImpl();
  }

  @BeforeEach
  void initTable() throws IOException {
    Utils.initTable(connection, Utils.NS_STR, "message", Arrays.asList(
        ColumnFamilyOption.from("m")
            .addParameter("TTL", "31536000")
            .addParameter("BLOOMFILTER", "NONE")
            .build()));
  }

  @AfterAll
  static void tearDown() throws IOException {
    connection.close();
  }

  @Test
  public void testSendAndGetInitialMessages() throws IOException {
    messagingService.sendMessage(1, 2, "r1u2");
    messagingService.sendMessage(1, 3, "r1u3");
    messagingService.sendMessage(1, 4, "r1u4");
    messagingService.sendMessage(2, 2, "r2u2");

    List<Message> messages = messagingService.getInitialMessages(1, null);
    assertEquals(3, messages.size());

    messages = messagingService.getInitialMessages(1, Arrays.asList(3L)); // block user ID: 3
    assertEquals(2, messages.size());
  }

  @Test
  public void testSendAndGetOldMessages() throws IOException {
    messagingService.sendMessage(1, 2, "r1u2");
    messagingService.sendMessage(1, 3, "r1u3");
    messagingService.sendMessage(1, 4, "r1u4");
    List<Message> messages = messagingService.getInitialMessages(1, null);
    Message oldMessage = messages.get(0); // should be "r1u4"

    messagingService.sendMessage(1, 5, "r1u5");
    messagingService.sendMessage(1, 6, "r1u6");
    messagingService.sendMessage(1, 7, "r1u7");
    messagingService.sendMessage(2, 2, "r2u2");

    messages = messagingService.getOldMessages(1, oldMessage, null);
    assertEquals(2, messages.size());

    messages = messagingService.getOldMessages(1, oldMessage, Arrays.asList(3L)); // block user ID: 3
    assertEquals(1, messages.size());
  }

  @Test
  public void testSendAndGetNewMessages() throws IOException {
    messagingService.sendMessage(1, 2, "r1u2");
    messagingService.sendMessage(1, 3, "r1u3");
    messagingService.sendMessage(1, 4, "r1u4");
    List<Message> messages = messagingService.getInitialMessages(1, null);
    Message newMessage = messages.get(0); // should be "r1u4"

    messagingService.sendMessage(1, 5, "r1u5");
    messagingService.sendMessage(1, 6, "r1u6");
    messagingService.sendMessage(1, 7, "r1u7");
    messagingService.sendMessage(2, 2, "r2u2");

    messages = messagingService.getNewMessages(1, newMessage, null);
    assertEquals(3, messages.size());

    messages = messagingService.getOldMessages(1, newMessage, Arrays.asList(6L)); // block user ID: 3
    assertEquals(2, messages.size());
  }

}
