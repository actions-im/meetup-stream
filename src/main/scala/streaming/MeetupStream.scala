package streaming

import core.Core
import org.apache.spark.streaming.dstream.DStream
import org.joda.time.DateTime
import org.json4s.DefaultFormats
import org.json4s._
import org.json4s.native.JsonMethods._
import org.joda.time.DateTime
import org.json4s.ParserUtil.ParseException



case class Venue(name: Option[String], address1: Option[String], city: Option[String], state: Option[String], zip: Option[String], country: Option[String], lon: Option[Float], lat: Option[Float]){
  def toExpandedCityString=List(city, state, country).flatMap{x=>x.map{_.toLowerCase()}}.mkString(",")
}
case class Event(name: Option[String], eventUrl: Option[String], description: Option[String], time: Option[DateTime], rsvpLimit: Option[Int])
case class Group(id: Option[String], category: Option[String], name: Option[String], city: Option[String], state: Option[String], country: Option[String])


object MeetupStream {
  
  import org.apache.spark.streaming.StreamingContext._
  
  implicit val formats = DefaultFormats
  
  def parsedStream(inputStream: DStream[String])={
    inputStream.map{ input=>
      val json=parse(input).camelizeKeys
      val venue=(json \ "venue").extract[Venue]
      //println(venue)
      val group=(json \ "group").extract[Group]
      //println(group)
      val event=json.extract[Event]
      //println(event)
      (venue, group, event)
    }
  }
  
  def countForLocaiton(counts: Seq[Int], initCount: Option[Int])=Some(initCount.getOrElse(0) + counts.sum)
  
  def mostActiveLocaitons(inputStream: DStream[String])=
    parsedStream(inputStream)
    .map{case(venue, group, event)=>(venue.toExpandedCityString,1)}
    //.reduceByKey{_+_}
    .updateStateByKey(countForLocaiton)
    .foreachRDD{
      rdd=>
        val top100=rdd.top(100)(Ordering.by{_._2})
        println("================================")
        println(top100.mkString("\n"))
        println("================================")
    }
    
        
           
}