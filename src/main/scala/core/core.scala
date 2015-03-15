package core

import org.apache.spark.streaming.StreamingContext
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.dstream.ReceiverInputDStream
import receiver.MeetupReceiver
import org.apache.spark.streaming.Minutes
import org.apache.spark.streaming.Milliseconds
import org.apache.spark.streaming.receiver.Receiver


trait Core {
  
  val ssc: StreamingContext
  
  lazy val sc=ssc.sparkContext
  
  val checkpointDirectory: String
  
  def createContext() : StreamingContext
  
  def createStreams(newSSC: StreamingContext)
  
}

trait BootedCore extends Core {
  
  Loggable.setStreamingLogLevels()

  val checkpointDirectory="./checkpoints/"
  
  val conf = new SparkConf(true)
      .setMaster("local[4]")
      .setAppName("MeetupExperiments")
              
  def createContext(): StreamingContext={
    val newSsc=new StreamingContext(conf, Seconds(1))
    createStreams(newSsc)
    newSsc
  }
    
  val ssc = StreamingContext.getOrCreate(checkpointDirectory, createContext, createOnError=false)
    
  ssc.start
  ssc.awaitTermination()  
      
}






