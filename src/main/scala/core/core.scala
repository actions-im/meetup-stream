package core

import org.apache.spark.streaming.StreamingContext
import org.apache.spark.SparkConf
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.dstream.ReceiverInputDStream
import receiver.MeetupReceiver
import org.apache.spark.streaming.Minutes
import org.apache.spark.streaming.Milliseconds
import org.apache.spark.streaming.receiver.Receiver
import scala.language.implicitConversions
import streaming.MeetupStream


trait Core {
  
  val ssc: StreamingContext
  
  val checkpointDirectory: String
  
  def createContext() : StreamingContext
  
  def createStreams(newSSC: StreamingContext)
  
  def withStreaming[T](f: StreamingContext=> T){
    try{      
      f(ssc)
      ssc.checkpoint(checkpointDirectory)
      ssc.start()
      ssc.awaitTermination()       
    }
    catch{
      case ex: Throwable => ssc.stop(true, true); ex.printStackTrace() 
    }
  }
  
}

trait BootedCore extends Core {
  
  Loggable.setStreamingLogLevels()
  import MeetupStream._
     
  
  val checkpointDirectory="./checkpoints/"
  
  val conf = new SparkConf(true)
      .setMaster("local[4]")
      .setAppName("MeetupExperiments")
              
  def createContext(): StreamingContext={
    val newSSC=new StreamingContext(conf, Seconds(1))
    createStreams(newSSC)
    newSSC
  }
    
  val ssc = StreamingContext.getOrCreate(checkpointDirectory, createContext, createOnError=false)
  
      
}

trait MeetupInput extends BootedCore{
  
  def processStreams(rsvpStream: DStream[String], eventsStream: DStream[String])
  
    
  def createStreams(ssc: StreamingContext)={
    val rsvpStream = ssc.receiverStream(new MeetupReceiver("http://stream.meetup.com/2/rsvps"))
    val eventsStream = ssc.receiverStream(new MeetupReceiver("http://stream.meetup.com/2/open_events"))
    processStreams(rsvpStream, eventsStream)
  }
    
  def withInputStreams(f: => Unit){
    withStreaming{ ssc=>
      f
    }
  }
    
}

