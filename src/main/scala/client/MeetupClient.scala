package client

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
import core.MeetupInput
import core.Loggable
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.SparkConf
import org.apache.spark.streaming.Seconds
import streaming.MeetupReceiver


object MeetupClient extends App {
  
          
  Loggable.setStreamingLogLevels()
  import MeetupStream._
     
  
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
  ssc.awaitTermination()

  
}

