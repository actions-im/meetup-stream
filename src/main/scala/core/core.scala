package core

import org.apache.spark.streaming.StreamingContext
import org.apache.spark.SparkConf
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.dstream.DStream
import streaming.MeetupReceiver
import org.apache.spark.streaming.Minutes
import org.apache.spark.streaming.Milliseconds
import org.apache.spark.streaming.receiver.Receiver
import scala.language.implicitConversions
import scala.reflect.ClassTag
import streaming.MeetupStream


trait Core {
  
  val ssc: StreamingContext
  
  val checkpointDirectory: String
  
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
  
  val conf = new SparkConf()
      .setMaster("local[4]")
      .setAppName("MeetupExperiments")
      .set("spark.driver.allowMultipleContexts", "true")
  
  
  def createContext(): StreamingContext={
    val newSSC=new StreamingContext(conf, Seconds(1))
    val inputStream = newSSC.receiverStream(new MeetupReceiver)
    mostActiveLocaitons(inputStream) 
    newSSC
  }
  
  
  val ssc = StreamingContext.getOrCreate(checkpointDirectory, createContext, createOnError=false)
    
}

trait MeetupInput{
  
  this: BootedCore =>     
    
  
  def withInputStream(f: DStream[String]=> Unit){
    withStreaming{ ssc=>
      //val inputStream = ssc.receiverStream(new MeetupReceiver)
      //f(ssc.inputStream)      
    }
  }
    
}

