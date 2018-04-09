package com.vurade.com.vurade.gcp

import java.util

import com.google.protobuf.ByteString
import com.google.pubsub.v1.TopicName

object Twitter2Pubsub {

  import com.google.api.core.ApiFuture
  import com.google.api.core.ApiFutures
  import com.google.cloud.pubsub.v1.Publisher
  import com.google.pubsub.v1.PubsubMessage
  import java.util

  val topicName: TopicName = TopicName.of("my-project-id", "my-topic-id")
  var publisher: Publisher = null
  val messageIdFutures = new util.ArrayList[ApiFuture[String]]

  try { // Create a publisher instance with default settings bound to the topic
    publisher = Publisher.newBuilder(topicName).build
    val messages = util.Arrays.asList("first message", "second message")
    // schedule publishing one message at a time : messages get automatically batched
    import scala.collection.JavaConversions._
    for (message <- messages) {
      val data = ByteString.copyFromUtf8(message)
      val pubsubMessage = PubsubMessage.newBuilder.setData(data).build
      // Once published, returns a server-assigned message id (unique within the topic)
      val messageIdFuture = publisher.publish(pubsubMessage)
      messageIdFutures.add(messageIdFuture)
    }
  } finally {
    // wait on any pending publish requests.
    val messageIds = ApiFutures.allAsList(messageIdFutures).get
    import scala.collection.JavaConversions._
    for (messageId <- messageIds) {
      System.out.println("published with message ID: " + messageId)
    }
    if (publisher != null) { // When finished with the publisher, shutdown to free up resources.
      publisher.shutdown()
    }
  }

}
