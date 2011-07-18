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
import org.apache.log4j.Logger
import collection.{JavaConversions, mutable}
import kafka.common.{InvalidMessageSizeException, ErrorMapping}

/**
 * A sequence of messages stored in a byte buffer
 * There are two ways to create a ByteBufferMessageSet 
 * Option 1: From a ByteBuffer which already contains the serialized message set. Consumers will use this method. 
 * Option 2: Give it a list of messages (scala/java) along with instructions relating to serialization format. Producers will use this method.
 * 
 */
class ByteBufferMessageSet protected () extends MessageSet {
  private val logger = Logger.getLogger(getClass())  
  private var validByteCount = -1
  private var buffer: ByteBuffer = null
  private var errorCode: Int = ErrorMapping.NoError
  private var shallowValidByteCount = -1
  private var deepValidByteCount = -1
  private var deepIterate = true

  def this(buffer: ByteBuffer, errorCode: Int, deepIterate: Boolean = true) = {
    this()
    this.buffer = buffer
    this.errorCode = errorCode
    this.deepIterate = deepIterate
  }
  
  def this(buffer: ByteBuffer) = this(buffer, ErrorMapping.NoError, true)

  def this(compressionEnabled: Boolean, messages: Message*) {
    this()
    if (compressionEnabled) {
      val message = CompressionUtils.compress(messages)
      buffer = ByteBuffer.allocate(message.serializedSize)
      message.serializeTo(buffer)
      buffer.rewind
    }
    else {
      buffer = ByteBuffer.allocate(MessageSet.messageSetSize(messages))
      for (message <- messages) {
        message.serializeTo(buffer)
      }
      buffer.rewind
    }
  }

  def this(compressionEnabled: Boolean, messages: Iterable[Message]) {
    this()
    if (compressionEnabled) {
      val message = CompressionUtils.compress(messages)
      buffer = ByteBuffer.allocate(message.serializedSize)
      message.serializeTo(buffer)
      buffer.rewind
    }
    else {
      buffer = ByteBuffer.allocate(MessageSet.messageSetSize(messages))
      for (message <- messages) {
        message.serializeTo(buffer)
      }
      buffer.rewind
    }
  }

  def enableDeepIteration() = {
    deepIterate = true
  }
  
  def disableDeepIteration() = {
    deepIterate = false
  }

  def getDeepIterate = deepIterate

  def getBuffer = buffer

  def getErrorCode = errorCode

  def serialized(): ByteBuffer = buffer

  def validBytes: Int = deepIterate match {
    case true => deepValidBytes
    case false => shallowValidBytes
  }
  
  def shallowValidBytes: Int = {
    if(shallowValidByteCount < 0) {
      val iter = shallowIterator
      while(iter.hasNext)
        iter.next()
    }
    shallowValidByteCount
  }
  
  def deepValidBytes: Int = {
    if (deepValidByteCount < 0) {
      val iter = deepIterator
      while (iter.hasNext)
        iter.next
    }
    deepValidByteCount
  }

  /** Write the messages in this set to the given channel */
  def writeTo(channel: WritableByteChannel, offset: Long, size: Long): Long =
    channel.write(buffer.duplicate)
  
  override def iterator: Iterator[Message] = deepIterate match {
    case true => deepIterator
    case false => shallowIterator
  }
  
  def shallowIterator(): Iterator[Message] = {
    ErrorMapping.maybeThrowException(errorCode)
    new IteratorTemplate[Message] {
      var iter = buffer.slice()
      var currValidBytes = 0
      
      override def makeNext(): Message = {
        // read the size of the item
        if(iter.remaining < 4) {
          shallowValidByteCount = currValidBytes
          return allDone()
        }
        val size = iter.getInt()
        if(size < 0 || iter.remaining < size) {
          shallowValidByteCount = currValidBytes
          if (currValidBytes == 0 || size < 0)
            throw new InvalidMessageSizeException("invalid message size:" + size + " only received bytes:" + iter.remaining
              + " at " + currValidBytes + " possible causes (1) a single message larger than the fetch size; (2) log corruption")
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


  def deepIterator(): Iterator[Message] = {
    ErrorMapping.maybeThrowException(errorCode)
    new IteratorTemplate[Message] {
      var topIter = buffer.slice()
      var currValidBytes = 0
      var innerIter:Iterator[Message] = null

      def innerDone():Boolean = (innerIter==null || !innerIter.hasNext)

      def makeNextOuter: Message = {
        if (topIter.remaining < 4) {
          deepValidByteCount = currValidBytes
          return allDone()
        }
        val size = topIter.getInt()
        logger.trace("Remaining bytes in iterator = " + topIter.remaining)
        logger.trace("size of data = " + size)
        if(size < 0 || topIter.remaining < size) {
          deepValidByteCount = currValidBytes
          if (currValidBytes == 0 || size < 0)
            throw new InvalidMessageSizeException("invalid message size:" + size + " only received bytes:" + topIter.remaining
              + " at " + currValidBytes + " possible causes (1) a single message larger than the fetch size; (2) log corruption")
          return allDone()
        }
        val message = topIter.slice()
        message.limit(size)
        topIter.position(topIter.position + size)
        val newMessage = new Message(message)
        newMessage.isCompressed match {
          case true=> {
            if(logger.isDebugEnabled)
              logger.debug("Message is compressed")
            innerIter = CompressionUtils.decompress(newMessage).deepIterator
            makeNext()
          }
          case false=> {
            if(logger.isDebugEnabled)
              logger.debug("Message is uncompressed")
            innerIter = null
            currValidBytes += 4 + size
            newMessage
          }
        }
      }

      override def makeNext(): Message = {
        logger.debug("makeNext() in deepIterator: innerDone = " + innerDone)
        innerDone match {
          case true => makeNextOuter
          case false => {
            val message = innerIter.next
            currValidBytes += message.serializedSize
            message
          }
        }
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
        (that canEqual this) && errorCode == that.errorCode && buffer.equals(that.buffer) &&
                deepIterate == that.deepIterate
      case _ => false
    }
  }

  override def canEqual(other: Any): Boolean = other.isInstanceOf[ByteBufferMessageSet]

  override def hashCode: Int = 31 + (17 * errorCode) + buffer.hashCode + deepIterate.hashCode
}
