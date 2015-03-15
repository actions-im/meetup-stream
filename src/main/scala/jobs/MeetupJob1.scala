package jobs

import org.apache.spark.SparkConf
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.Seconds
import receiver.MeetupReceiver
import core.Loggable
import org.apache.spark.mllib.clustering.KMeans
import org.apache.spark.mllib.linalg.Vectors
import scala.io.Source
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.linalg.Vector
import core._


object MeetupJob1 extends App{
  
  import streaming.Parsing._
  import org.apache.spark.streaming.StreamingContext._
  import core.Loggable._
  import transformations.FeatureExtraction._

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
  
  //static
  val eventsHistory = ssc.sparkContext.textFile("data/events/events.json", 1).flatMap(parseEvent)
      
  val dictionary= ssc.sparkContext.broadcast(localDictionary)
        
  val eventVectors=eventsHistory.flatMap{event=>eventToVector(dictionary.value,event.description.getOrElse(""))}
  
  eventVectors.cache()
  
  val eventCount=eventVectors.count()
  
  println(s"Training on ${eventCount} events")
   
  val eventClusters = KMeans.train(eventVectors, 10, 2)
  //end static
  
  println(s"Training Complete")
  
  ssc.checkpoint("checkpoints")  
      
  val eventHistoryById=eventsHistory
    .map{event=>(event.id, event.description.getOrElse(""))}
    .reduceByKey{(first: String, second: String)=>first}
  
  println(s"Joining with ${eventHistoryById.count}")
  
  eventHistoryById.checkpoint()
  
  eventHistoryById.cache()

  object PureFunctions{
    

    def groupMembers(memberResponses: Seq[(Member, String)], initList: Option[Set[Member]]) = {
      val initialMemberList=initList.getOrElse(Set())
      val newMemberList=(memberResponses :\ initialMemberList) {case((member, response), memberList) =>
        if (response == "yes") memberList + member else memberList - member  
      }
      if (newMemberList.size>0) Some(newMemberList) else None
    }  
       
  }
 
   val membersByEventId=rsvpStream
     .flatMap{
       case(member, memberEvent, response) => 
         memberEvent.eventId.map{id=>(id,(member, response))}
     }
  
   
  val rsvpEventInfo=membersByEventId.transform(
      rdd=>rdd.join(eventHistoryById)      
    )  
         
  val memberEventInfo = rsvpEventInfo
    .flatMap{
    case(eventId, ((member, response), description)) => {
     eventToVector(dictionary.value,description).map{ eventVector=> 
       val eventCluster=eventClusters.predict(eventVector)
       (eventCluster,(member, response))
     }
    }}
  
  val memberGroups = memberEventInfo.updateStateByKey(PureFunctions.groupMembers)
  
  //memberGroups.print(10)
  
  val recomendations=memberEventInfo
    .join(memberGroups)
    .map{case(cluster, ((member, memberResponse), members)) => (member.memberName, members-member)}
  
  recomendations.print(10)
  
  ssc.start
  ssc.awaitTermination()  
  
}