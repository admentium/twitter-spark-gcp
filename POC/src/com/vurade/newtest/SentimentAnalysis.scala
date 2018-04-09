package com.vurade.newtest

import com.vurade.streamtest.Utils._
import org.apache.spark._
import org.apache.spark.streaming._
import org.apache.spark.streaming.twitter._
import org.apache.log4j.{Level, Logger}
import org.apache.spark.streaming.dstream.DStream
import twitter4j.Status

object SentimentAnalysis {
  def main(args: Array[String]) {

    System.setProperty("twitter4j.oauth.consumerKey", "22qbDErnsdCjLDn4M6bSdvRoH")
    System.setProperty("twitter4j.oauth.consumerSecret", "eZ9584b0rL56Oj7Vr7aZBeRIYa4O0DCvnVOvuONPQAfPfLsuXo")
    System.setProperty("twitter4j.oauth.accessToken", "730796749938253824-5xQ6CeaIKZxYGxaH9T35Nb2yKP00zLN")
    System.setProperty("twitter4j.oauth.accessTokenSecret", "H9VfU1Di9jsRVU6tZFPKT7TwdargYeKv4gzq8L3Vfq8LK")

    val sparkConfiguration = new SparkConf().
      setAppName("spark-twitter-stream-example").
      setMaster("local[*]")

    val sparkContext = new SparkContext(sparkConfiguration)
    val ssc = new StreamingContext(sparkContext, Seconds(5))

    if (!Logger.getRootLogger.getAllAppenders.hasMoreElements) {
      Logger.getRootLogger.setLevel(Level.WARN)
    }

    val uselessWords = sparkContext.broadcast(load("resources/stop-words.dat"))
    val positiveWords = sparkContext.broadcast(load("resources/pos-words.dat"))
    val negativeWords = sparkContext.broadcast(load("resources/neg-words.dat"))

    val filters = Array("Mayawati")

    val tweets: DStream[Status] = TwitterUtils.createStream(ssc, None, filters)

    val textAndSentences: DStream[(TweetText, Sentence)] =
      tweets.
        map(_.getText).
        map(tweetText => (tweetText, wordsOf(tweetText)))

    val textAndMeaningfulSentences: DStream[(TweetText, Sentence)] =
      textAndSentences.
        mapValues(toLowercase).
        mapValues(keepActualWords).
        mapValues(words => keepMeaningfulWords(words, uselessWords.value)).
        filter { case (_, sentence) => sentence.length > 0 }

    // Compute the score of each sentence and keep only the non-neutral ones
    val textAndNonNeutralScore: DStream[(TweetText, Int)] =
      textAndMeaningfulSentences.
        mapValues(sentence => computeScore(sentence, positiveWords.value, negativeWords.value)).
        filter { case (_, score) => score != 0 }

    // Transform the (tweet, score) pair into a readable string and print it
    textAndNonNeutralScore.map(makeReadable).saveAsTextFiles("resources/NonNeutral/1", "txt")

   ssc.start()
    ssc.awaitTermination()
  }
}
