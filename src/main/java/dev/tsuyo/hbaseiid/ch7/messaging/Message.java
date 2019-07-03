package dev.tsuyo.hbaseiid.ch7.messaging;

public class Message {
  private String messageId;
  private long userId;
  private String userName;
  private String body;
  private long postAt;

  public String getMessageId() {
    return messageId;
  }

  public void setMessageId(String messageId) {
    this.messageId = messageId;
  }

  public long getUserId() {
    return userId;
  }

  public void setUserId(long userId) {
    this.userId = userId;
  }

  public String getUserName() {
    return userName;
  }

  public void setUserName(String userName) {
    this.userName = userName;
  }

  public String getBody() {
    return body;
  }

  public void setBody(String body) {
    this.body = body;
  }

  public long getPostAt() {
    return postAt;
  }

  public void setPostAt(long postAt) {
    this.postAt = postAt;
  }
}
