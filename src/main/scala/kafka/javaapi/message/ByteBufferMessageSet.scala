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
import kafka.message.{Message, MessageSet}
import java.nio.channels.WritableByteChannel

class ByteBufferMessageSet(val buffer: ByteBuffer, val errorCode: Int) extends MessageSet {
  val underlying = new kafka.message.ByteBufferMessageSet(buffer, errorCode)

  def this(buffer: ByteBuffer) = this(buffer,ErrorMapping.NO_ERROR)

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

  def validBytes: Int = underlying.validBytes

  def writeTo(channel: WritableByteChannel, offset: Long, size: Long): Long =
      underlying.writeTo(channel, offset, size)

  override def iterator: Iterator[Message] = underlying.iterator

  override def toString: String = underlying.toString

  def sizeInBytes: Long = underlying.sizeInBytes

  override def equals(other: Any): Boolean = {
    other match {
      case that: ByteBufferMessageSet =>
        (that canEqual this) && errorCode == that.errorCode && buffer.equals(that.buffer)
      case _ => false
    }
  }

  override def canEqual(other: Any): Boolean = other.isInstanceOf[ByteBufferMessageSet]

  override def hashCode: Int = 31 * (17 + errorCode) + buffer.hashCode

}