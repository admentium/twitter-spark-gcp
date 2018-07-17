package com.vurade.streamtest

import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.twitter.TwitterUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}
import twitter4j.Status
import Utils._
import twitter4j.auth.AuthorizationFactory
import twitter4j.conf.ConfigurationContext

object TwitterSentimentScore {

  def main(args: Array[String]) {

    val sparkConfiguration = new SparkConf().
    setAppName("spark-twitter-stream-example").
    setMaster("local[*]")


  // Let's create the Spark Context using the configuration we just created
  val sparkContext = new SparkContext(sparkConfiguration)

  // Now let's wrap the context in a streaming one, passing along the window size
  val streamingContext = new StreamingContext(sparkContext, Seconds(5))


    val twitterConf = ConfigurationContext.getInstance
    val twitterAuth = AuthorizationFactory.getInstance(twitterConf)

  // Creating a stream from Twitter (see the README to learn how to
  // provide a configuration to make this work - you'll basically
  // need a set of Twitter API keys)
  val tweets: DStream[Status] =
  TwitterUtils.createStream(streamingContext, None)

  // To compute the sentiment of a tweet we'll use different set of words used to
  // filter and score each word of a sentence. Since these lists are pretty small
  // it can be worthwhile to broadcast those across the cluster so that every
  // executor can access them locally
  val uselessWords = sparkContext.broadcast(load("resources/stop-words.dat"))
  val positiveWords = sparkContext.broadcast(load("resources/pos-words.dat"))
  val negativeWords = sparkContext.broadcast(load("resources/neg-words.dat"))

  // Let's extract the words of each tweet
  // We'll carry the tweet along in order to print it in the end
  val textAndSentences: DStream[(TweetText, Sentence)] =
  tweets.
    map(_.getText).
    map(tweetText => (tweetText, wordsOf(tweetText)))

  // Apply several transformations that allow us to keep just meaningful sentences
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
  textAndNonNeutralScore.map(makeReadable).print

  // Now that the streaming is defined, start it
  streamingContext.start()

  // Let's await the stream to end - forever
  streamingContext.awaitTermination()

}
}