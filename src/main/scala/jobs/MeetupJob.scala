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
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import transformations.FeatureExtraction
import core._

/*
object MeetupJob {// extends App{
  
  import streaming.Parsing._
  import org.apache.spark.streaming.StreamingContext._
  import core.Loggable._
  import FeatureExtraction._
  
  
  def createStreams(ssc: StreamingContext)={
    
    val rsvpStream = ssc.receiverStream(new MeetupReceiver("http://stream.meetup.com/2/rsvps")).flatMap(parseRsvp)
    
    val membersByEventId=rsvpStream
     .flatMap{
       case(member, memberEvent, response) => 
         memberEvent.eventId.map{id=>(id,(member, response))}
     }
    
     
    def groupMembers(memberResponses: Seq[(Member, String)], initList: Option[Set[Member]]) = {
      val initialMemberList=initList.getOrElse(Set())
      val newMemberList=(memberResponses :\ initialMemberList) {
        case((member, response), memberList) =>
          if (response == "yes") memberList + member else memberList - member  
      }
      if (newMemberList.size>0) Some(newMemberList) else None
    }  
     
    val memberEventInfo = membersByEventId
     .transform(rdd=>rdd.join(eventHistoryById))
     .flatMap{
       case(eventId, ((member, response), event)) => {
        eventToVector(dictionary.value,event).map{ eventVector=> 
          val eventCluster=eventClusters.predict(eventVector)
          (eventCluster,(member, response))
        }}
     }
    
    val memberClasters=memberEventInfo.updateStateByKey(groupMembers)
      
     memberEventInfo.print     
  }
    
  
  Loggable.setStreamingLogLevels()
  
  val conf = new SparkConf(true)
      .setMaster("local[4]")
      .setAppName("MeetupExperiments")
              
  def createContext(): StreamingContext={
    val newSsc=new StreamingContext(conf, Seconds(1))
    createStreams(newSsc)
    newSsc
  }
    
  val ssc = StreamingContext.getOrCreate("./checkpoints/", createContext, createOnError=false)

  val eventsHistory = ssc.sparkContext.textFile("data/events/events.json", 1).flatMap(parseEvent)
    
  val dictionary=ssc.sparkContext.broadcast(localDictionary)
    
  ssc.checkpoint("checkpoints")
    
  val eventVectors=eventsHistory.flatMap{event=>eventToVector(dictionary.value,event)}
  eventVectors.cache      
  val eventCount=eventVectors.count()
  println(s"Training on ${eventCount} events")
  val eventClusters = KMeans.train(eventVectors, 10, 2)
  println(s"Training Complete")    
 
  val eventHistoryById=eventsHistory.flatMap{event=>event.id.map(id=>(id, event))}    
    
  ssc.start
  ssc.awaitTermination()   
    
         
 
}

*/