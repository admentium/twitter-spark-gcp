package com.vurade.newtest

import org.apache.log4j.{Level, Logger}

import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.twitter._
import org.apache.spark.SparkConf

/**
  * Calculates popular hashtags (topics) over sliding 10 and 60 second windows from a Twitter
  * stream. The stream is instantiated with credentials and optionally filters supplied by the
  * command line arguments.
  *
  * Run this on your local machine as
  *
  */
object TwitterPopularTags {
  def main(args: Array[String]) {
       // Set logging level if log4j not configured (override by adding log4j.properties to classpath)
    if (!Logger.getRootLogger.getAllAppenders.hasMoreElements) {
      Logger.getRootLogger.setLevel(Level.WARN)
    }

    // Set the system properties so that Twitter4j library used by twitter stream
    // can use them to generate OAuth credentials
    System.setProperty("twitter4j.oauth.consumerKey", "22qbDErnsdCjLDn4M6bSdvRoH")
    System.setProperty("twitter4j.oauth.consumerSecret", "eZ9584b0rL56Oj7Vr7aZBeRIYa4O0DCvnVOvuONPQAfPfLsuXo")
    System.setProperty("twitter4j.oauth.accessToken", "730796749938253824-5xQ6CeaIKZxYGxaH9T35Nb2yKP00zLN")
    System.setProperty("twitter4j.oauth.accessTokenSecret", "H9VfU1Di9jsRVU6tZFPKT7TwdargYeKv4gzq8L3Vfq8LK")

    val sparkConf = new SparkConf().setAppName("TwitterPopularTags")

    // check Spark configuration for master URL, set it to local if not configured
    if (!sparkConf.contains("spark.master")) {
      sparkConf.setMaster("local[2]")
    }
    val filters = Array("Car")
    val ssc = new StreamingContext(sparkConf, Seconds(2))
    val stream = TwitterUtils.createStream(ssc, None, filters)

    val hashTags = stream.flatMap(status => status.getText.split(" ").filter(_.startsWith("#")))

    val topCounts60 = hashTags.map((_, 1)).reduceByKeyAndWindow(_ + _, Seconds(60))
      .map{case (topic, count) => (count, topic)}
      .transform(_.sortByKey(false))

    val topCounts10 = hashTags.map((_, 1)).reduceByKeyAndWindow(_ + _, Seconds(10))
      .map{case (topic, count) => (count, topic)}
      .transform(_.sortByKey(false))


    // Print popular hashtags
    topCounts60.foreachRDD(rdd => {
      val topList = rdd.take(10)
      println("\nPopular topics in last 60 seconds (%s total):".format(rdd.count()))
      topList.foreach{case (count, tag) => println("%s (%s tweets)".format(tag, count))}
    })

    topCounts10.foreachRDD(rdd => {
      val topList = rdd.take(10)
      println("\nPopular topics in last 10 seconds (%s total):".format(rdd.count()))
      topList.foreach{case (count, tag) => println("%s (%s tweets)".format(tag, count))}
    })

    ssc.start()
    ssc.awaitTermination()
  }
}
