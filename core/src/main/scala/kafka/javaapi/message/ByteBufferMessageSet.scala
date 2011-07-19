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
package kafka.javaapi.message

import java.nio.ByteBuffer
import kafka.common.ErrorMapping
import org.apache.log4j.Logger
import kafka.message._

class ByteBufferMessageSet(val buffer: ByteBuffer,
                           val errorCode: Int = ErrorMapping.NoError,
                           val deepIterate: Boolean = true) extends MessageSet {
  private val logger = Logger.getLogger(getClass())
  val underlying: kafka.message.ByteBufferMessageSet = new kafka.message.ByteBufferMessageSet(buffer, errorCode, deepIterate)
//  var buffer:ByteBuffer = null
//  var errorCode:Int = ErrorMapping.NoError
//  var deepIterate = false

  def this(buffer: ByteBuffer) = this(buffer, ErrorMapping.NoError, true)

  def this(compressionCodec: CompressionCodec, messages: java.util.List[Message]) {
    this(compressionCodec match {
      case NoCompressionCodec =>
        val buffer = ByteBuffer.allocate(MessageSet.messageSetSize(messages))
        val messageIterator = messages.iterator
        while(messageIterator.hasNext) {
          val message = messageIterator.next
          message.serializeTo(buffer)
        }
        buffer.rewind
        buffer
      case _ =>
        import scala.collection.JavaConversions._
        val message = CompressionUtils.compress(asBuffer(messages), compressionCodec)
        val buffer = ByteBuffer.allocate(message.serializedSize)
        message.serializeTo(buffer)
        buffer.rewind
        buffer
    }, ErrorMapping.NoError, true)
  }

  def validBytes: Int = underlying.validBytes

  def serialized():ByteBuffer = underlying.serialized

  override def iterator: java.util.Iterator[Message] = new java.util.Iterator[Message] {
    val underlyingIterator = underlying.iterator
    override def hasNext(): Boolean = {
      underlyingIterator.hasNext
    }

    override def next(): Message = {
      underlyingIterator.next
    }

    override def remove = throw new UnsupportedOperationException("remove API on MessageSet is not supported")
  }

  override def toString: String = underlying.toString

  def sizeInBytes: Long = underlying.sizeInBytes

  override def equals(other: Any): Boolean = {
    other match {
      case that: ByteBufferMessageSet =>
        (that canEqual this) && errorCode == that.errorCode && buffer.equals(that.buffer) &&
                                             deepIterate == that.deepIterate
      case _ => false
    }
  }

  def canEqual(other: Any): Boolean = other.isInstanceOf[ByteBufferMessageSet]

  override def hashCode: Int = 31 * (17 + errorCode) + buffer.hashCode + deepIterate.hashCode

}