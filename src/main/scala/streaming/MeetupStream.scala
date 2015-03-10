package streaming

import core.Core
import org.apache.spark.streaming.dstream.DStream
import org.joda.time.DateTime
import org.json4s.DefaultFormats
import org.json4s._
import org.json4s.native.JsonMethods._
import org.joda.time.DateTime
import org.json4s.ParserUtil.ParseException
import org.apache.spark.Partitioner
import org.apache.spark.streaming.Seconds
import scala.util.Try

object MeetupStream {
  
  case class Venue(name: Option[String], address1: Option[String], city: Option[String], state: Option[String], zip: Option[String], country: Option[String], lon: Option[Float], lat: Option[Float]){
    def toExpandedCityString=List(city, state, country).flatMap{x=>x.map{_.toLowerCase()}}.mkString(",")
  }
  case class Event(id: Option[String], name: Option[String], eventUrl: Option[String], description: Option[String], time: Option[Long], rsvpLimit: Option[Int])
  case class Group(id: Option[String], category: Option[String], name: Option[String], city: Option[String], state: Option[String], country: Option[String])
  
  case class Member(memberName: Option[String], memberId: Option[String])
  case class MemberEvent(eventId: Option[String], eventName: Option[String], eventUrl: Option[String], time: Option[Long])  
  
  import org.apache.spark.streaming.StreamingContext._
  
  implicit val formats = DefaultFormats
  
  def parseEvent(eventJson: String)={
    Try({
      val json=parse(eventJson).camelizeKeys
      val venue=(json \ "venue").extract[Venue]
      val group=(json \ "group").extract[Group]
      val event=json.extract[Event]
      //(venue, group, event)
      event      
    }).toOption
  }
  
  def parseRsvp(rsvpJson: String)={
    Try({
      val json=parse(rsvpJson).camelizeKeys
      val member=(json \ "member").extract[Member]
      val event=(json \ "event").extract[MemberEvent]
      val response=(json \ "response").extract[String]
      (member, event, response)
    }).toOption
  }
  
  def parsedEventStream(eventStream: DStream[String])=eventStream.map(parseEvent)
    
  def parsedRsvpStream(rsvpStream: DStream[String])= rsvpStream.map(parseRsvp)
  
  def countForLocaiton(counts: Seq[Int], initCount: Option[Int])=Some(initCount.getOrElse(0) + counts.sum)
  
  /*
  def mostActiveLocaitons(inputStream: DStream[String]) = {
    val stateStream=parsedStream(inputStream)
    .map{case(venue, group, event)=>(venue.toExpandedCityString,1)}
    .updateStateByKey(countForLocaiton)
    
    stateStream
    .checkpoint(Seconds(5))    
    .foreachRDD{
      rdd=>
        val top100=rdd.top(100)(Ordering.by{_._2})
        println("================================")
        println(top100.mkString("\n"))
        println("================================")
    }
  }*/
    
        
           
}