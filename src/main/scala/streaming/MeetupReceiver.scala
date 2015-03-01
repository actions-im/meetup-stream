/**
 *
 */
package streaming

import org.apache.spark.streaming.receiver.Receiver
import org.apache.spark.storage.StorageLevel
import org.apache.spark.Logging
import com.ning.http.client.AsyncHttpClientConfig
import com.ning.http.client._
import scala.collection.mutable.ArrayBuffer
import java.io.OutputStream
import java.io.ByteArrayInputStream
import java.io.InputStreamReader
import java.io.BufferedReader
import java.io.InputStream
import java.io.PipedInputStream
import java.io.PipedOutputStream
/**
 * @author szelvenskiy
 *
 */
class MeetupReceiver extends Receiver[String](StorageLevel.MEMORY_AND_DISK_2) with Logging {
  
  @transient var client: AsyncHttpClient = _
  
  @transient var inputPipe: PipedInputStream = _
  @transient var outputPipe: PipedOutputStream = _  
       
  def onStart() {
    val url="http://stream.meetup.com/2/open_events"
    val cf = new AsyncHttpClientConfig.Builder()
    cf.setRequestTimeout(Integer.MAX_VALUE)
    cf.setReadTimeout(Integer.MAX_VALUE)
    cf.setPooledConnectionIdleTimeout(Integer.MAX_VALUE)      
    client= new AsyncHttpClient(cf.build())
    
    inputPipe = new PipedInputStream(1024 * 1024)
    outputPipe = new PipedOutputStream(inputPipe)
    val producerThread = new Thread(new DataConsumer(inputPipe))
    producerThread.start()
    
    client.prepareGet(url).execute(new AsyncHandler[Unit]{
        
      def onBodyPartReceived(bodyPart: HttpResponseBodyPart) = {
        bodyPart.writeTo(outputPipe)
        AsyncHandler.STATE.CONTINUE        
      }
      
      def onStatusReceived(status: HttpResponseStatus) = {
        AsyncHandler.STATE.CONTINUE
      }
      
      def onHeadersReceived(headers: HttpResponseHeaders) = {
        AsyncHandler.STATE.CONTINUE
      }
            
      def onCompleted = {
        println("completed")
      }
      
      
      def onThrowable(t: Throwable)={
        t.printStackTrace()
      }
        
    })    
    
    
  }

  def onStop() {
    if (Option(client).isDefined) client.close()
    outputPipe.flush();
    outputPipe.close();
  }
  
  class DataConsumer(inputStream: InputStream) extends Runnable 
  {
       
      override
      def run()
      {        
        val bufferedReader = new BufferedReader( new InputStreamReader( inputStream ))
        var input=bufferedReader.readLine()
        while(input!=null){          
          store(input)
          input=bufferedReader.readLine()
        }            
      }  
      
  }

}