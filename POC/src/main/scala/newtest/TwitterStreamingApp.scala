package com.vurade.newtest

import com.google.cloud.pubsub.v1.Publisher
import com.google.protobuf.ByteString
import com.google.pubsub.v1.{PubsubMessage, TopicName}
import org.apache.spark.streaming.{Seconds, StreamingContext}

import org.apache.spark.SparkConf
import org.apache.spark.streaming.twitter.TwitterUtils

import scala.collection.mutable.ArrayBuffer
import org.apache.log4j.PropertyConfigurator
import org.apache.spark.streaming.dstream.DStream


object TwitterStreamingApp {

  def main(args: Array[String]) {
    val baseDir = System.getProperty("user.dir")
    PropertyConfigurator.configure(baseDir + "resources/log4j.properties");


    // Set the system properties so that Twitter4j library used by twitter stream
    // can use them to generate OAuth credentials
    System.setProperty("twitter4j.oauth.consumerKey", "22qbDErnsdCjLDn4M6bSdvRoH")
    System.setProperty("twitter4j.oauth.consumerSecret", "eZ9584b0rL56Oj7Vr7aZBeRIYa4O0DCvnVOvuONPQAfPfLsuXo")
    System.setProperty("twitter4j.oauth.accessToken", "730796749938253824-5xQ6CeaIKZxYGxaH9T35Nb2yKP00zLN")
    System.setProperty("twitter4j.oauth.accessTokenSecret", "H9VfU1Di9jsRVU6tZFPKT7TwdargYeKv4gzq8L3Vfq8LK")

    //val sparkConf = new SparkConf().setAppName("TwitterPopularTags")
    // to run this application inside an IDE, comment out previous line and uncomment line below
    val sparkConf = new SparkConf().setAppName("TwitterPopularTags")

    val ssc = new StreamingContext(sparkConf, Seconds(2))

    ssc.checkpoint(baseDir + "resources/checkpoint")

    val filters = Array("SteveSmith","Hichki","#RamNavami")

    val stream = TwitterUtils.createStream(ssc, None, filters);
//    val topic: TopicName = TopicName.of("natural-oath-198915", "projects/natural-oath-198915/topics/tweets4us")
//    var publisher: Publisher = null

    val stopWords = ssc.sparkContext.textFile("resources/stop-words.dat").collect().toSet
    val positiveWords = ssc.sparkContext.textFile("resources/pos-words.dat").collect().toSet
    val negativeWords = ssc.sparkContext.textFile("resources/neg-words.dat").collect().toSet

    val englishTweets = stream.filter(status => status.getLang() == "en")
    val tweets = englishTweets.map(status => status.getText)
    val location = englishTweets.map(status => status.getGeoLocation)
    val usr = englishTweets.map(status => status.getUser)

    // create a pair of (tweet, array of words) for each tweet
    val tweetAndWords: DStream[(String, Array[String])] = tweets
      .map(x => (x, x.split(" ")))
      .mapValues(filterWords(_, stopWords))



    // create a pair of (tweet, sentiment) for each tweet
    val tweetAndSentiment: DStream[(String, String)] = tweetAndWords.mapValues(getSentiment(_, positiveWords, negativeWords))

//    tweetAndWords.foreachRDD(rdd => {
//      for (item <- rdd.collect()){
//        import scala.collection.JavaConversions._
//        publisher = Publisher.newBuilder(topicName).build
//        val data = ByteString.copyFromUtf8(item._1)
//        val pubsubMessage = PubsubMessage.newBuilder.setData(data).build
//        // Once published, returns a server-assigned message id (unique within the topic)
//        publisher.publish(pubsubMessage)
//        println(item._2)
//      }
//    })
    // Maintain # of positive, negative and neutral sentiment counts for 10 seconds window
    val sentimentCountsWindow10 = tweetAndSentiment
      .map(x => (x._2, 1))
      .reduceByKeyAndWindow((a:Int,b:Int) => (a + b), Seconds(10), Seconds(10))

    // Maintain # of positive, negative and neutral sentiment counts for 30 seconds window
    val sentimentCountsWindow30 = tweetAndSentiment
      .map(x => (x._2, 1))
      .reduceByKeyAndWindow((a:Int,b:Int) => (a + b), Seconds(30), Seconds(30))



    // print results to console
    tweetAndSentiment.saveAsTextFiles("resources/out/1","txt")
    sentimentCountsWindow10.saveAsTextFiles("resources/out/2","txt")
    sentimentCountsWindow30.saveAsTextFiles("resources/out/3","txt")

    ssc.start()
    ssc.awaitTermination()
  }

//  def publishMessages(): Unit = {
//    // [START pubsub_publish]
//    val topic: TopicName = TopicName.of("my-project-id", "my-topic-id")
//    var publisher: Publisher = null
//    val messageIdFutures: List[ApiFuture[String]] = new ArrayList[ApiFuture[String]]()
//    publisher = Publisher.newBuilder(topicName).build


  def filterWords(words: Array[String], stopWords: Set[String]): Array[String] = {
    val punctuationMarks = "[.,!?\"\\n]".r
    val firstLastNonChars = "^\\W|\\W$".r
    val filteredWords = words
      .map(_.toLowerCase())
      .map(punctuationMarks.replaceAllIn(_, ""))
      .map(firstLastNonChars.replaceAllIn(_, ""))
      .filter(!_.isEmpty())
      .filter(!stopWords.contains(_))

    return filteredWords
  }

  def getSentiment(wordsArray: Array[String], positiveWords: Set[String], negativeWords: Set[String]): String = {
    var numPositive = 0;
    var numNegative = 0;
    var numNeutral = 0;

    for (words <- wordsArray){
      if (positiveWords.contains(words))
        numPositive += 1
      else if (negativeWords.contains(words))
        numNegative += 1
      else
        numNeutral +=1
    }

    if ((numPositive - numNegative) > 0)
      return "Positive"
    else if ((numPositive - numNegative) < 0)
      return "Negative"
    else
      return "Neutral"
  }
}