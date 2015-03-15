package util

import core._
import org.joda.time.DateTime
import org.json4s.DefaultFormats
import org.json4s._
import org.json4s.native.JsonMethods._
import org.joda.time.DateTime
import org.apache.spark.Partitioner
import org.apache.spark.streaming.Seconds
import scala.util.Try

object Parsing {
  
  
  @transient implicit val formats = DefaultFormats
  
  def parseEvent(eventJson: String):Option[Event]={
    Try({
      val json=parse(eventJson).camelizeKeys
      val event=json.extract[Event]
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
             
}