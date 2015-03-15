package streaming

import core._
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

object Parsing {
  
  
  //import org.apache.spark.streaming.StreamingContext._
  
  @transient implicit val formats = DefaultFormats
  
  def parseEvent(eventJson: String):Option[Event]={
    Try({
      val json=parse(eventJson).camelizeKeys
      //val venue=(json \ "venue").extract[Venue]
      //val group=(json \ "group").extract[Group]
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
             
}