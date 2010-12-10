/*
 * Copyright 2010 LinkedIn
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package kafka.message

import java.nio._
import java.nio.channels._
import scala.collection.mutable
import kafka.message._
import kafka.utils._
import kafka.common.ErrorMapping
import org.apache.log4j.Logger

/**
 * A sequence of messages stored in a byte buffer
 */
class ByteBufferMessageSet(val buffer: ByteBuffer, val errorCOde: Int) extends MessageSet {
  private val logger = Logger.getLogger(getClass())  
  private var validByteCount = -1

  def this(buffer: ByteBuffer) = this(buffer,ErrorMapping.NO_ERROR)
  
  def this(messages: Message*) {
    this(ByteBuffer.allocate(MessageSet.messageSetSize(messages)))
    for(message <- messages) {
      buffer.putInt(message.size)
      buffer.put(message.buffer)
      message.buffer.rewind()
    }
    buffer.rewind()
  }
  
  def this(messages: java.util.List[Message]) {
    this(ByteBuffer.allocate(MessageSet.messageSetSize(messages)))
    val iter = messages.iterator
    while(iter.hasNext) {
      val message = iter.next.asInstanceOf[Message]
      buffer.putInt(message.size)
      buffer.put(message.buffer)
      message.buffer.rewind()
    }
    buffer.rewind()
  }
  
  def validBytes: Int = {
    if(validByteCount < 0) {
      val iter = iterator
      while(iter.hasNext)
        iter.next()
    }
    validByteCount
  }

  /** Write the messages in this set to the given channel */
  def writeTo(channel: WritableByteChannel, offset: Long, size: Long): Long = 
    channel.write(buffer.duplicate)
  
  override def iterator: Iterator[Message] = {
    ErrorMapping.maybeThrowException(errorCOde)
    new IteratorTemplate[Message] {
      var iter = buffer.slice()
      var currValidBytes = 0
      
      override def makeNext(): Message = {
        // read the size of the item
        if(iter.remaining < 4) {
          validByteCount = currValidBytes
          return allDone()
        }
        val size = iter.getInt()
        if(iter.remaining < size) {
          validByteCount = currValidBytes
          if (currValidBytes == 0)
            logger.warn("consumer fetch size too small? expected size:" + size + " received bytes:" + iter.remaining)
          return allDone()
        }
        currValidBytes += 4 + size
        val message = iter.slice()
        message.limit(size)
        iter.position(iter.position + size)
        new Message(message)
      }
    }
  }

  def sizeInBytes: Long = buffer.limit
  
  override def toString: String = {
    val builder = new StringBuilder()
    builder.append("ByteBufferMessageSet(")
    for(message <- this) {
      builder.append(message)
      builder.append(", ")
    }
    builder.append(")")
    builder.toString
  }

  override def equals(other: Any): Boolean = {
    other match {
      case that: ByteBufferMessageSet =>
        (that canEqual this) && errorCOde == that.errorCOde && buffer.equals(that.buffer) 
      case _ => false
    }
  }

  override def canEqual(other: Any): Boolean = other.isInstanceOf[ByteBufferMessageSet]

  override def hashCode: Int = 31 * (17 + errorCOde) + buffer.hashCode
}
