package com.vurade.newtest

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.streaming._
import org.apache.spark.streaming.twitter._
import org.apache.spark.streaming.StreamingContext._
import twitter4j.conf.ConfigurationBuilder
import twitter4j.Twitter
import twitter4j.TwitterFactory
import twitter4j.auth.Authorization
import twitter4j.auth.AuthorizationFactory
import twitter4j.conf.Configuration
import twitter4j.conf.ConfigurationContext
import org.apache.log4j.{Level, Logger}

object TwitterStream {

  /** Our main function where the action happens */
  def main(args: Array[String]) {

    System.setProperty("twitter4j.oauth.consumerKey", "22qbDErnsdCjLDn4M6bSdvRoH")
    System.setProperty("twitter4j.oauth.consumerSecret", "eZ9584b0rL56Oj7Vr7aZBeRIYa4O0DCvnVOvuONPQAfPfLsuXo")
    System.setProperty("twitter4j.oauth.accessToken", "730796749938253824-5xQ6CeaIKZxYGxaH9T35Nb2yKP00zLN")
    System.setProperty("twitter4j.oauth.accessTokenSecret", "H9VfU1Di9jsRVU6tZFPKT7TwdargYeKv4gzq8L3Vfq8LK")

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("TwitterPopularTags")
    val ssc = new StreamingContext(sparkConf,Seconds(5))

    if (!Logger.getRootLogger.getAllAppenders.hasMoreElements) {
      Logger.getRootLogger.setLevel(Level.WARN)
    }


    val filters = Array("Mayawati")

   val tweets = TwitterUtils.createStream(ssc, None, filters)

    val engTweets = tweets.filter(x => x.getLang() == "en")

    val statuses = engTweets.map(status => status.getText).saveAsTextFiles("resources/tweetsTwitter/1","txt")
//
//
//
//    val tweetwords = statuses.flatMap(tweetText => tweetText.split(" "))
//
//    val hashtags = tweetwords.filter(word => word.startsWith("#"))
//
//    val hashtagKeyValues = hashtags.map(hashtag => (hashtag, 1)) //


//    val hashtagCounts =
//      hashtagKeyValues.reduceByKeyAndWindow((x:Int,y:Int)=>x+y, Seconds(5),
//        Seconds(20))
//    val sortedResults = hashtagCounts.transform(rdd => rdd.sortBy(x =>
//      x._2, false))
  //  sortedResults.saveAsTextFiles("resources/tweetsTwitter","txt")

//    sortedResults.print



   // ssc.checkpoint("resources/checkpointTwitter")
    ssc.start()
    ssc.awaitTermination()
  }
}