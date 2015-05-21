package com.ikaver.phrases

import java.util.Comparator
import java.util.function.{ToDoubleFunction, ToIntFunction, ToLongFunction, Function}

import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkContext, SparkConf}

import scala.collection.mutable.{ArrayBuffer}

object ScoreOrdering extends Ordering[(String, Double, Double, Double)] {
  override def compare(x: (String, Double, Double, Double), y: (String, Double, Double, Double)): Int = {
    y._2 compare x._2
  }
}

object Phrases {
  def main(args: Array[String]) {

    val stopWords = Set("a", "about", "above", "across", "after", "afterwards", "again", "against", "all", "almost", "alone", "along", "already", "also", "although", "always", "am", "among", "amongst", "amoungst", "amount", "an", "and", "another", "any", "anyhow", "anyone", "anything", "anyway", "anywhere", "are", "around", "as", "at", "back", "be", "became", "because", "become", "becomes", "becoming", "been", "before", "beforehand", "behind", "being", "below", "beside", "besides", "between", "beyond", "bill", "both", "bottom", "but", "by", "call", "can", "cannot", "cant", "co", "computer", "con", "could", "couldnt", "cry", "de", "describe", "detail", "do", "done", "down", "due", "during", "each", "eg", "eight", "either", "eleven", "else", "elsewhere", "empty", "enough", "etc", "even", "ever", "every", "everyone", "everything", "everywhere", "except", "few", "fifteen", "fify", "fill", "find", "fire", "first", "five", "for", "former", "formerly", "forty", "found", "four", "from", "front", "full", "further", "get", "give", "go", "had", "has", "hasnt", "have", "he", "hence", "her", "here", "hereafter", "hereby", "herein", "hereupon", "hers", "herself", "him", "himself", "his", "how", "however", "hundred", "i", "ie", "if", "in", "inc", "indeed", "interest", "into", "is", "it", "its", "itself", "keep", "last", "latter", "latterly", "least", "less", "ltd", "made", "many", "may", "me", "meanwhile", "might", "mill", "mine", "more", "moreover", "most", "mostly", "move", "much", "must", "my", "myself", "name", "namely", "neither", "never", "nevertheless", "next", "nine", "no", "nobody", "none", "noone", "nor", "not", "nothing", "now", "nowhere", "of", "off", "often", "on", "once", "one", "only", "onto", "or", "other", "others", "otherwise", "our", "ours", "ourselves", "out", "over", "own", "part", "per", "perhaps", "please", "put", "rather", "re", "same", "see", "seem", "seemed", "seeming", "seems", "serious", "several", "she", "should", "show", "side", "since", "sincere", "six", "sixty", "so", "some", "somehow", "someone", "something", "sometime", "sometimes", "somewhere", "still", "such", "system", "take", "ten", "than", "that", "the", "their", "them", "themselves", "then", "thence", "there", "thereafter", "thereby", "therefore", "therein", "thereupon", "these", "they", "thick", "thin", "third", "this", "those", "though", "three", "through", "throughout", "thru", "thus", "to", "together", "too", "top", "toward", "towards", "twelve", "twenty", "two", "un", "under", "until", "up", "upon", "us", "very", "via", "was", "we", "well", "were", "what", "whatever", "when", "whence", "whenever", "where", "whereafter", "whereas", "whereby", "wherein", "whereupon", "wherever", "whether", "which", "while", "whither", "who", "whoever", "whole", "whom", "whose", "why", "will", "with", "within", "without", "would", "yet", "you", "your", "yours", "yourself", "yourselves")

    val phraseWeight = args(0).toDouble
    val infoWeight = args(1).toDouble
    val numBigrams = args(2).toInt
    val numMappers = args(3).toInt
    val numReducers = args(4).toInt
    val fgYear = args(5).toInt
    val bigramsPath = args(6)
    val unigramsPath = args(7)
    val stopWordsPath = args(8)


    val sparkConf = new SparkConf().setAppName("Phrases").setMaster(s"local[$numMappers]")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")

    val sc = new SparkContext(sparkConf)
    val bigramsRaw = sc.textFile(bigramsPath)
    val bigramTokens = bigramsRaw.map{ line =>
      val tokens = line.split("\t")
      val isFgYear = tokens(1).toInt == fgYear
      val count = tokens(2).toInt
      if(isFgYear) (tokens(0), (count, 0))
      else (tokens(0), (0, count))
    }.filter{ x =>
      val words = x._1.split(" ")
      !(stopWords.contains(words(0)) || stopWords.contains(words(1)))
    }
    val aggregatedBigrams = bigramTokens.reduceByKey({(x:(Int,Int), y:(Int,Int)) =>
      (x._1+y._1, x._2+y._2)
    }, numReducers)
    val processedBigrams = aggregatedBigrams.map{ x =>
      val bigram = x._1
      val words = bigram.split(" ")
      (words(0), (words(1), x._2))
    }.persist()

    val totalBigrams = processedBigrams.count()

    val totalBigramCounts = processedBigrams.map{ x => (x._2._2._1.toLong, x._2._2._2.toLong) }.reduce{(x:(Long,Long), y:(Long,Long)) =>
      (x._1+y._1, x._2+y._2)
    }

    val unigramsRaw = sc.textFile(unigramsPath)
    val unigramTokens = unigramsRaw.map{ line =>
      val tokens = line.split("\t")
      val isFgYear = tokens(1).toInt == fgYear
      val count = tokens(2).toInt
      if(isFgYear) (tokens(0), (count, 0))
      else (tokens(0), (0, count))
    }.filter{x => !stopWords.contains(x._1)}
    val processedUnigrams = unigramTokens.reduceByKey({(x:(Int,Int), y:(Int,Int)) =>
      (x._1+y._1, x._2+y._2)
    }, numReducers).persist()

    val totalUnigrams = processedUnigrams.count()

    val totalUnigramCounts = processedUnigrams.map{ x => (x._2._1.toLong, x._2._2.toLong) }.reduce{(x:(Long,Long), y:(Long,Long)) =>
      (x._1+y._1, x._2+y._2)
    }

    val groupedByFirstWord = processedUnigrams.cogroup(processedBigrams)
    val bigramsWithFirstWordCount : RDD[(String, (String, Int, Int, Int))] = groupedByFirstWord.flatMap{ x =>
      val firstWord = x._1
      val unigramIter = x._2._1
      val bigramIter = x._2._2
      val unigramCounts = unigramIter.head
      val buf = ArrayBuffer[(String, (String, Int, Int, Int))]()
      for( bigram <- bigramIter) {
        val bigramFG = bigram._2._1
        val bigramBG = bigram._2._2
        val newTuple = (bigram._1, (firstWord, bigramFG, bigramBG, unigramCounts._1))
        buf += newTuple
      }
      buf.toList
    }

    val groupedBySecondWord = processedUnigrams.cogroup(bigramsWithFirstWordCount)
    val bigramsWithUnigramData: RDD[(String, String, Int, Int, Int, Int)] = groupedBySecondWord.flatMap{ x =>
      val secondWord = x._1
      val unigramIter = x._2._1
      val bigramIter = x._2._2
      val unigramCounts = unigramIter.head
      val buf = ArrayBuffer[(String, String, Int, Int, Int, Int)]()
      bigramIter.foreach{ bigram =>
        val bigramFG = bigram._2
        val bigramBG = bigram._3
        val firstFG = bigram._4
        val newTuple = (bigram._1, secondWord, bigramFG, bigramBG, firstFG, unigramCounts._1)
        buf += newTuple
      }
      buf.toList
    }

    val totalBigramsFG = totalBigramCounts._1
    val totalBigramsBG = totalBigramCounts._2
    val totalUnigramsFG = totalUnigramCounts._1

    val scoresOfBigrams = bigramsWithUnigramData.map{ x =>
      def klDivergence(p: Double, q: Double) = {
        p * (Math.log(p) - Math.log(q))
      }
      val firstWord = x._1
      val secondWord = x._2
      val bigramFG = x._3
      val bigramBG = x._4
      val firstFG = x._5
      val secondFG = x._6

      val pBigramFG = (bigramFG + 1).toDouble / (0 + totalBigramsFG).toDouble
      val pBigramBG = (bigramBG + 1).toDouble / (0 + totalBigramsBG).toDouble
      val pFirstFG  = (firstFG  + 1).toDouble / (0 + totalUnigramsFG).toDouble
      val pSecondFG = (secondFG + 1).toDouble / (0 + totalUnigramsFG).toDouble

      val phraseness = klDivergence(pBigramFG, pFirstFG * pSecondFG)
      val informativeness = klDivergence(pBigramFG, pBigramBG)
      (firstWord + " " + secondWord, phraseWeight * phraseness + infoWeight * informativeness, phraseness, informativeness)
    }

    scoresOfBigrams.takeOrdered(numBigrams)(ScoreOrdering).foreach{ x =>
      val bigram = x._1
      val totalScore = x._2
      val phrasenessScore = x._3
      val informativenessScore = x._4
      println(f"$bigram\t$totalScore%.5f\t$phrasenessScore%.5f\t$informativenessScore%.5f")
    }

    sc.stop()
  }
}
