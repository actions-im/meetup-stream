package jobs

import com.ning.http.client._
import scala.concurrent._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.util.Success
import java.io.InputStream
import java.io.ByteArrayOutputStream
import java.io.PipedInputStream
import java.io.PipedOutputStream
import java.io.ByteArrayInputStream
import java.io.IOException
import java.io.BufferedReader
import java.io.InputStreamReader
import java.io.BufferedInputStream
import java.nio.channels.AsynchronousSocketChannel
import java.net.InetSocketAddress
import java.nio.ByteBuffer
import core.BootedCore
import streaming.MeetupStream
import core.Loggable
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.SparkConf
import org.apache.spark.streaming.Seconds
import receiver.MeetupReceiver
/*import org.apache.spark.streaming.scheduler.StreamingListener
import org.apache.spark.streaming.scheduler.StreamingListenerBatchCompleted
import org.apache.spark.streaming.scheduler.StreamingListenerBatchStarted
import org.apache.spark.streaming.scheduler.StreamingListenerReceiverStarted*/
import org.apache.spark.streaming.dstream.DStream


//object MeetupJob extends MeetupInput  {//}extends App with MeetupInput {
  
  /*        
  Loggable.setStreamingLogLevels()
  
     
  
  val checkpointDirectory="./checkpoints/"
  
  val conf = new SparkConf()
      .setMaster("local[4]")
      .setAppName("MeetupExperiments")
  
  
  def createContext(): StreamingContext={
    val newSsc=new StreamingContext(conf, Seconds(1))
    val inputStream = newSsc.receiverStream(new MeetupReceiver)
    mostActiveLocaitons(inputStream) 
    newSsc
  }
    
  val ssc = StreamingContext.getOrCreate(checkpointDirectory, createContext, createOnError=false)
  
  ssc.checkpoint(checkpointDirectory)
  ssc.start
  ssc.awaitTermination() */
  import MeetupStream._

  /*  
  override
  def processStreams(rsvpStream: DStream[String], eventsStream: DStream[String])={
    //parsedEventStream(eventsStream).print
    rsvpStream.print
    //parsedRsvpStream(rsvpStream).print
  }
  
 
  withInputStreams{
    ssc.addStreamingListener(new StreamingListener{
      
      override
      def onReceiverStarted(receiverStarted: StreamingListenerReceiverStarted) {
       println(receiverStarted.receiverInfo) 
      }
      
      override
      def onBatchStarted(batchStarted: StreamingListenerBatchStarted)={
        println(batchStarted.batchInfo)
        
      }
      
      override
      def onBatchCompleted(batchCompleted: StreamingListenerBatchCompleted)={
        println(batchCompleted.batchInfo)
      }
                  
    })
    
  } */
  
//}

