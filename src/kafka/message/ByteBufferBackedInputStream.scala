package kafka.message

import java.io.InputStream
import java.nio.ByteBuffer
import scala.Math

class ByteBufferBackedInputStream(buffer:ByteBuffer) extends InputStream {
  override def read():Int  = { 
          if (!buffer.hasRemaining()) 
          { 
            -1
          }
         (buffer.get() & 0xFF)
        }
  
  override def read(bytes:Array[Byte], off:Int, len:Int):Int = { 
          if (!buffer.hasRemaining()) {
            -1
            }
          // Read only what's left 
          val realLen = math.min(len, buffer.remaining())
          buffer.get(bytes, off, realLen) 
          realLen 
          }   
}