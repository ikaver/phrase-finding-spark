package com.ikaver.phrases

import org.apache.spark.SparkContext._
import org.apache.spark.{SparkContext, SparkConf}


object Phrases {
  def main(args: Array[String]) {
    val sparkConf = new SparkConf().setAppName("Phrases")
    val phraseWeight = args(0).toDouble
    val infoWeight = args(1).toDouble
    val numBigrams = args(2).toInt
    val numMappers = args(3).toInt
    val numReducers = args(4).toInt
    val fgYear = args(5).toInt
    val bigramsPath = args(6)
    val unigramsPath = args(7)
    val stopWordsPath = args(8)

    val sc = new SparkContext(sparkConf)
    println(args)
    val bigramsRaw = sc.textFile(bigramsPath, numMappers)
    val bigramTokens = bigramsRaw.map{ line =>
      val tokens = line.split("\t")
      val isFgYear = tokens(1).toInt == fgYear
      val count = tokens(2).toInt
      if(isFgYear) (tokens(0), (count, 0))
      else (tokens(0), (0, count))
    }
    val aggregatedBigrams = bigramTokens.reduceByKey({(x:(Int,Int), y:(Int,Int)) =>
      (x._1+y._1, x._2+y._2)
    }, numReducers)
    val processedBigrams = aggregatedBigrams.map{ x =>
      val bigram = x._1
      val words = bigram.split(" ")
      (words(0), (words(1), x._2))
    }.cache()

    val totalBigramCounts = processedBigrams.map{ x => x._2._2 }.reduce{(x:(Int,Int), y:(Int,Int)) =>
      (x._1+y._1, x._2+y._2)
    }

    println(processedBigrams.collect())
    println(totalBigramCounts._1)
    println(totalBigramCounts._2)

    sc.stop()
  }
}
