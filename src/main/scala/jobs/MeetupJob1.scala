package jobs

import org.apache.spark.SparkConf
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.Seconds
import receiver.MeetupReceiver
import core.Loggable
import scala.collection.SortedMap
import scala.collection.immutable.ListMap
import org.apache.spark.mllib.clustering.KMeans
import org.apache.spark.mllib.linalg.Vectors
import scala.io.Source
import org.apache.spark.mllib.feature.{HashingTF, IDF}
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.linalg.Vector


object MeetupJob1 extends App{
  
  import streaming.MeetupStream._
  import org.apache.spark.streaming.StreamingContext._
  import core.Loggable._

  
  val conf = new SparkConf()
      .setMaster("local[4]")
      .setAppName("MeetupExperiments")
      .set("spark.executor.memory", "1g")
      .set("spark.driver.memory", "1g")
      
  Loggable.setStreamingLogLevels()
  
  val localDictionary=Source
    .fromURL(getClass.getResource("/wordsEn.txt"))
    .getLines
    .zipWithIndex
    .toMap
        
  val ssc=new StreamingContext(conf, Seconds(1))
  val rsvpStream = ssc.receiverStream(new MeetupReceiver("http://stream.meetup.com/2/rsvps")).flatMap(parseRsvp)
  //val eventStream = ssc.receiverStream(new MeetupReceiver("http://stream.meetup.com/2/open_events"))
    
  
  //static
  val eventsHistory = ssc.sparkContext.textFile("data/events/events.json", 1).flatMap(parseEvent)
  val rsvpHistory = ssc.sparkContext.textFile("data/rsvps/rsvps.json", 1).map(parseRsvp)
      
  val dictionary= ssc.sparkContext.broadcast(localDictionary)
  
  
  def breakToWords(description: String)={
    val wordSelector="""[^\<\>\/]\b([a-zA-Z\d]{3,})\b""".r
    (wordSelector findAllIn description).map{_.trim.toLowerCase()}
  }
  
  
  def popularWords(words: Iterator[String])={
    val initialWordCounts=collection.mutable.Map[String, Int]()
    val wordCounts=words.foldLeft(initialWordCounts){case(wordCounts, word)=> wordCounts+Tuple2(word, wordCounts.getOrElse(word,0)+1)}
    //println(wordCounts)
    val wordsIndexes=wordCounts      
      .flatMap{
        case(word, count)=>localDictionary.get(word).map{index=>(index,count.toDouble)}
      }    
    val topWords=wordsIndexes.toSeq.sortBy(-1*_._2).take(10)    
    topWords
  }
  
  def eventToVector(event: Event): Option[Vector]={
   val wordsIterator = event.description.map(breakToWords).getOrElse(Iterator())
   val topWords=popularWords(wordsIterator)
   if (topWords.size==10) Some(Vectors.sparse(dictionary.value.size,topWords)) else None
  }
  
  
  /*  
  val eventDescriptions=eventsHistory
    .flatMap{event=>event.description}
    .map(text=>breakToWords(text).toSeq)
        
  val eventVectors=new HashingTF().transform(eventDescriptions)
  * */

  
  //eventVectors.cache()
  
  //val idf = new IDF().fit(eventVectors)
  //val eventsTfIdf: RDD[Vector] = idf.transform(eventVectors)

      
  val eventVectors=eventsHistory.flatMap{event=>eventToVector(event)}
  
  eventVectors.cache()
  
  val eventCount=eventVectors.count()
  
  println(s"Training on ${eventCount} events")
   
  val eventClusters = KMeans.train(eventVectors, 10, 2)
  //end static
  
  println(s"Training Complete")
  
  ssc.checkpoint("checkpoints")  
      
 val eventHistoryById=eventsHistory.flatMap{event=>event.id.map(id=>(id, event))}
  
 println(s"Joining with ${eventHistoryById.count}")
  
 eventHistoryById.checkpoint() 
  
  val membersByEventId=rsvpStream
   .map{case(member, memberEvent, response)=>(memberEvent.eventId, member, response)}
   .filter{case(eventIdOption, member, response) => eventIdOption.isDefined}
   .map{case (eventIdOption, member, response) => (eventIdOption.get, (member, response))}
      
  
  def countNewMembers(memberResponses: Seq[(Member, String)], initList: Option[Set[Member]])={
    val initialMemberList=initList.getOrElse(Set())
    val newMemberList=(memberResponses :\ initialMemberList) {case((member, response), memberList) =>
      if (response == "yes") memberList + member else memberList - member  
    }
    if (newMemberList.size>0) Some(newMemberList) else None
  }
  
  object PureFunctions{
    

    def groupMembers(memberResponses: Seq[(Member, String)], initList: Option[Set[Member]]) = {
      val initialMemberList=initList.getOrElse(Set())
      val newMemberList=(memberResponses :\ initialMemberList) {case((member, response), memberList) =>
        if (response == "yes") memberList + member else memberList - member  
      }
      if (newMemberList.size>0) Some(newMemberList) else None
    }  
    
    def eventToVector(event: Event): Vector={
     val document = event.description.map(breakToWords).getOrElse(Iterator()).toSeq
     new HashingTF().transform(document)
    }
    
    
  }
 
  val eventMembers=membersByEventId
   .updateStateByKey(countNewMembers)
   
  val memberEventInfo = membersByEventId.transform(
      rdd=>rdd.join(eventHistoryById)      
    )
    .flatMap{
    case(eventId, ((member, response), event)) => {
     eventToVector(event).map{ eventVector=> 
       val eventCluster=eventClusters.predict(eventVector)
       (eventCluster,(member, response))
     }
    }}
    .updateStateByKey(PureFunctions.groupMembers)
    .print 
  
  ssc.start
  ssc.awaitTermination()  
  
}