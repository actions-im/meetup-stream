package transformations

import scala.io.Source
import core._
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.rdd.RDD
import org.apache.spark.broadcast.Broadcast



object FeatureExtraction  {
  
  val localDictionary=Source
    .fromURL(getClass.getResource("/wordsEn.txt"))
    .getLines
    .zipWithIndex
    .toMap  
  
    
  def breakToWords(description: String)={
    val wordSelector="""[^\<\>\/]\b([a-zA-Z\d]{4,})\b""".r
    (wordSelector findAllIn description).map{_.trim.toLowerCase()}
  }
  
  
  def eventToVector(dictionary: Map[String, Int], description: String): Option[Vector]={
    
  def popularWords(words: Iterator[String])={
    val initialWordCounts=collection.mutable.Map[String, Int]()
    val wordCounts=words.
      foldLeft(initialWordCounts){
        case(wordCounts, word)=> wordCounts+Tuple2(word,wordCounts.getOrElse(word,0)+1)
      }
    val wordsIndexes=wordCounts     
     .flatMap{
        case(word, count)=>dictionary.get(word).map{index=>(index,count.toDouble)}
      }
    val topWords=wordsIndexes.toSeq.sortBy(-1*_._2).take(10)
    topWords
  }
    
    
    
   val wordsIterator = breakToWords(description)
   val topWords=popularWords(wordsIterator)   
   if (topWords.size==10) Some(Vectors.sparse(dictionary.size,topWords)) else None
  }
  
}