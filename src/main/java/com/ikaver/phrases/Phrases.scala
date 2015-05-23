package com.ikaver.phrases

import java.util.Comparator
import java.util.function.{ToDoubleFunction, ToIntFunction, ToLongFunction, Function}

import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkContext, SparkConf}

import scala.collection.mutable.{ListBuffer}

object ScoreOrdering extends Ordering[(String, Double, Double, Double)] {
  override def compare(x: (String, Double, Double, Double), y: (String, Double, Double, Double)): Int = {
    y._2 compare x._2
  }
}

object Phrases {
  def main(args: Array[String]) {
    //assumed:
    //structure of bigrams  file is one bigram  per line, with format bigram[TAB]year[TAB]count
    //structure of unigrams file is one unigram per line, with format unigram[TAB]year[TAB]count

    //stop words. Cleaner solution would read these from a file :)
    val stopWords = Set("a", "about", "above", "across", "after", "afterwards", "again", "against", "all", "almost", "alone", "along", "already", "also", "although", "always", "am", "among", "amongst", "amoungst", "amount", "an", "and", "another", "any", "anyhow", "anyone", "anything", "anyway", "anywhere", "are", "around", "as", "at", "back", "be", "became", "because", "become", "becomes", "becoming", "been", "before", "beforehand", "behind", "being", "below", "beside", "besides", "between", "beyond", "bill", "both", "bottom", "but", "by", "call", "can", "cannot", "cant", "co", "computer", "con", "could", "couldnt", "cry", "de", "describe", "detail", "do", "done", "down", "due", "during", "each", "eg", "eight", "either", "eleven", "else", "elsewhere", "empty", "enough", "etc", "even", "ever", "every", "everyone", "everything", "everywhere", "except", "few", "fifteen", "fify", "fill", "find", "fire", "first", "five", "for", "former", "formerly", "forty", "found", "four", "from", "front", "full", "further", "get", "give", "go", "had", "has", "hasnt", "have", "he", "hence", "her", "here", "hereafter", "hereby", "herein", "hereupon", "hers", "herself", "him", "himself", "his", "how", "however", "hundred", "i", "ie", "if", "in", "inc", "indeed", "interest", "into", "is", "it", "its", "itself", "keep", "last", "latter", "latterly", "least", "less", "ltd", "made", "many", "may", "me", "meanwhile", "might", "mill", "mine", "more", "moreover", "most", "mostly", "move", "much", "must", "my", "myself", "name", "namely", "neither", "never", "nevertheless", "next", "nine", "no", "nobody", "none", "noone", "nor", "not", "nothing", "now", "nowhere", "of", "off", "often", "on", "once", "one", "only", "onto", "or", "other", "others", "otherwise", "our", "ours", "ourselves", "out", "over", "own", "part", "per", "perhaps", "please", "put", "rather", "re", "same", "see", "seem", "seemed", "seeming", "seems", "serious", "several", "she", "should", "show", "side", "since", "sincere", "six", "sixty", "so", "some", "somehow", "someone", "something", "sometime", "sometimes", "somewhere", "still", "such", "system", "take", "ten", "than", "that", "the", "their", "them", "themselves", "then", "thence", "there", "thereafter", "thereby", "therefore", "therein", "thereupon", "these", "they", "thick", "thin", "third", "this", "those", "though", "three", "through", "throughout", "thru", "thus", "to", "together", "too", "top", "toward", "towards", "twelve", "twenty", "two", "un", "under", "until", "up", "upon", "us", "very", "via", "was", "we", "well", "were", "what", "whatever", "when", "whence", "whenever", "where", "whereafter", "whereas", "whereby", "wherein", "whereupon", "wherever", "whether", "which", "while", "whither", "who", "whoever", "whole", "whom", "whose", "why", "will", "with", "within", "without", "would", "yet", "you", "your", "yours", "yourself", "yourselves")

    val phraseWeight = args(0).toDouble //how much weight do we give to phraseness in the final score
    val infoWeight = args(1).toDouble   //how much weight do we give to informativeness in the final score
    val numBigrams = args(2).toInt      //how many bigrams to include in the final result (top K bigrams)
    val numMappers = args(3).toInt      //how many mappers to use
    val numReducers = args(4).toInt     //how many reducers to use
    val fgYear = args(5).toInt          //what is the year that we are considering to be the foreground (FG)
    val bigramsPath = args(6)           //the path to the bigrams file
    val unigramsPath = args(7)          //the path to the unigrams file

    //our goal here is to construct an RDD in the form of
    //(bigram, bigramFGCount, bigramBGCount, firstWordFGCount, secondWordFGCount)
    //so we can easily compute the phraseness and informativeness of each bigram

    //initialize the spark context
    //might need to tweak values such as spark.driver.memory to get good performance here
    val sparkConf = new SparkConf().setAppName("Phrases").setMaster(s"local[$numMappers]")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    val sc = new SparkContext(sparkConf)

    //get RDD of bigrams in form (bigram, fgCount, bgCount). Remove bigrams with stop words.
    val bigramsRaw = sc.textFile(bigramsPath)
    val bigramTokens = bigramsRaw.map{ line =>
      val tokens = line.split("\t")
      val isFgYear = tokens(1).toInt == fgYear
      val count = tokens(2).toInt
      if(isFgYear) (tokens(0), (count, 0))
      else (tokens(0), (0, count))
    }.filter{ x =>
      val bigram = x._1
      val words = bigram.split(" ")
      !(stopWords.contains(words(0)) || stopWords.contains(words(1)))
    }
    //aggregate bigrams with the same key (sum all FG and BG counts of the same bigrams)
    val aggregatedBigrams = bigramTokens.reduceByKey({(x:(Int,Int), y:(Int,Int)) =>
      (x._1+y._1, x._2+y._2)
    }, numReducers)

    //map bigrams to have the first word as the key, to later group with the unigrams RDD
    val processedBigrams = aggregatedBigrams.map{ x =>
      val (bigram, counts) = x
      val words = bigram.split(" ")
      (words(0), (words(1), counts))
    }.persist()

    //get uniqueBigrams, totalBigramsFG and totalBigramsBG counts for smoothing
    val uniqueBigramsCount = processedBigrams.count()
    val (totalBigramsFG, totalBigramsBG)  = processedBigrams.map{ x => (x._2._2._1.toLong, x._2._2._2.toLong) }
      .reduce{(x:(Long,Long), y:(Long,Long)) => (x._1+y._1, x._2+y._2) }

    //get RDD of unigrams in form (unigram, fgCount, bgCount). Remove unigrams with stop words.
    val unigramsRaw = sc.textFile(unigramsPath)
    val unigramTokens = unigramsRaw.map{ line =>
      val tokens = line.split("\t")
      val isFgYear = tokens(1).toInt == fgYear
      val count = tokens(2).toInt
      if(isFgYear) (tokens(0), (count, 0))
      else (tokens(0), (0, count))
    }.filter{x => !stopWords.contains(x._1)}

    //aggregate unigrams with the same key (sum all FG and BG counts of the same unigram)
    val processedUnigrams = unigramTokens.reduceByKey({(x:(Int,Int), y:(Int,Int)) =>
      (x._1+y._1, x._2+y._2)
    }, numReducers).persist()

    //get uniqueUnigrams, totalUnigramsFG counts for smoothing
    val uniqueUnigramsCount = processedUnigrams.count()
    val (totalUnigramsFG, _) = processedUnigrams.map{ x => (x._2._1.toLong, x._2._2.toLong) }
      .reduce{(x:(Long,Long), y:(Long,Long)) => (x._1+y._1, x._2+y._2) }

    //group the processedUnigrams RDD with the processedBigrams RDD to get the counts of the
    //first word of the bigram with the bigram counts. We will later do the same for the second word of the bigram.
    val groupedByFirstWord = processedUnigrams.cogroup(processedBigrams)
    val bigramsWithFirstWordCount : RDD[(String, (String, Int, Int, Int))] = groupedByFirstWord.flatMap{ x =>
      val (firstWord, (unigramIter, bigramIter)) = x
      val (unigramFG, _) = unigramIter.head
      bigramIter.map{ bigram =>
        val (secondWord, (bigramFG, bigramBG)) = bigram
        //use the second word of the bigram as the key to later to do the same for the second word
        (secondWord, (firstWord, bigramFG, bigramBG, unigramFG))
      }
    }

    //group the bigramsWithFirstWordCount with the processedUnigrams RDD to get the counts
    //of the second word of the bigram with the bigram counts.
    //After this step we will finally get the RDD wanted (firstWord, secondWord, bigramFG, bigramBG, firstFG, secondFG)
    val groupedBySecondWord = processedUnigrams.cogroup(bigramsWithFirstWordCount)
    val bigramsWithUnigramData: RDD[(String, String, Int, Int, Int, Int)] = groupedBySecondWord.flatMap{ x =>
      val (secondWord, (unigramIter, bigramIter)) = x
      val (unigramFG, _) = unigramIter.head
      bigramIter.map{ bigram =>
        val (firstWord, bigramFG, bigramBG, firstFG) = bigram
        (firstWord, secondWord, bigramFG, bigramBG, firstFG, unigramFG)
      }
    }

    //scoresOfBigrams indicates the final score of each bigram
    val scoresOfBigrams = bigramsWithUnigramData.map{ x =>
      def klDivergence(p: Double, q: Double) = {
        p * (Math.log(p) - Math.log(q))
      }
      val (firstWord, secondWord, bigramFG, bigramBG, firstFG, secondFG) = x

      //use smoothing to calculate probabilities
      val pBigramFG = (bigramFG + 1).toDouble / (uniqueBigramsCount + totalBigramsFG).toDouble
      val pBigramBG = (bigramBG + 1).toDouble / (uniqueBigramsCount + totalBigramsBG).toDouble
      val pFirstFG  = (firstFG  + 1).toDouble / (uniqueUnigramsCount + totalUnigramsFG).toDouble
      val pSecondFG = (secondFG + 1).toDouble / (uniqueUnigramsCount + totalUnigramsFG).toDouble

      val phraseness = klDivergence(pBigramFG, pFirstFG * pSecondFG)
      val informativeness = klDivergence(pBigramFG, pBigramBG)
      (firstWord + " " + secondWord, phraseWeight * phraseness + infoWeight * informativeness, phraseness, informativeness)
    }

    //take the top K bigrams and print them to stdout
    scoresOfBigrams.takeOrdered(numBigrams)(ScoreOrdering).foreach{ x =>
      val (bigram, totalScore, phrasenessScore, informativenessScore) = x
      println(f"$bigram\t$totalScore%.5f\t$phrasenessScore%.5f\t$informativenessScore%.5f")
    }

    sc.stop()
  }
}
