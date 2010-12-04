package kafka.message

import java.io.ByteArrayOutputStream
import java.io.IOException
import java.io.InputStream
import java.util.zip.GZIPInputStream
import java.util.zip.GZIPOutputStream
import java.nio.ByteBuffer

object CompressionUtils {
  var DEFAULT_COMPRESSION_CODEC = 1;
  //0 is reserved to indicate no compression
  val GZIP_COMPRESSION = 1;
  
  def compress(messages:Iterable[Message]):Message = compress(messages, DEFAULT_COMPRESSION_CODEC)
  
  def compress(messages:Iterable[Message], compressionCodec:Int):Message = compressionCodec match {
    case GZIP_COMPRESSION =>
      val outputStream:ByteArrayOutputStream = new ByteArrayOutputStream()
      val gzipOutput:GZIPOutputStream = new GZIPOutputStream(outputStream)
      val messageByteBuffer = ByteBuffer.allocate(MessageSet.messageSetSize(messages))
      for (message <- messages) {
        message.serializeTo(messageByteBuffer)
      }
      messageByteBuffer.rewind
      gzipOutput.write(messageByteBuffer.array)
      gzipOutput.close();
      outputStream.close();
      val oneCompressedMessage:Message = new Message(outputStream.toByteArray,compressionCodec)
      oneCompressedMessage
    case _ => 
      print("Unknown Codec: " + compressionCodec)
      throw new Exception()
  }
  
  def decompress(message:Message):ByteBufferMessageSet = message.compressionCodec match {
    case GZIP_COMPRESSION =>
      val outputStream:ByteArrayOutputStream = new ByteArrayOutputStream
      val inputStream:InputStream = new ByteBufferBackedInputStream(message.payload)    
      val gzipIn:GZIPInputStream = new GZIPInputStream(inputStream)
      val intermediateBuffer = new Array[Byte](1024)
      var len=gzipIn.read(intermediateBuffer)
      while (len >0) {
        outputStream.write(intermediateBuffer,0,len)
        len = gzipIn.read(intermediateBuffer)      
      }
      
      gzipIn.close
      outputStream.close
      val outputBuffer = ByteBuffer.allocate(outputStream.size)
      outputBuffer.put(outputStream.toByteArray)
      outputBuffer.rewind
      val outputByteArray = outputStream.toByteArray
      new ByteBufferMessageSet(outputBuffer)
    case _ => 
      print("Unknown Codec: " + message.compressionCodec)
      throw new Exception()
  }

}