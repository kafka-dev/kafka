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
import kafka.message.Message
import org.apache.log4j.Logger

class ByteBufferMessageSet protected () extends MessageSet {
  private val logger = Logger.getLogger(getClass())
  var underlying: kafka.message.ByteBufferMessageSet = null
  var buffer:ByteBuffer = null
  var errorCode:Int = ErrorMapping.NO_ERROR
  var deepIterate = false

  def this(buffer: ByteBuffer, errorCode: Int, deepIterate: Boolean) = {
    this()
    this.buffer = buffer
    this.errorCode = errorCode
    this.deepIterate = deepIterate
    this.underlying = new kafka.message.ByteBufferMessageSet(this.buffer, this.errorCode, this.deepIterate)
  }

  def this(buffer: ByteBuffer) = this(buffer, ErrorMapping.NO_ERROR, false)

  import scala.collection.JavaConversions._
  def this(compressionEnabled: Boolean = false, messages: java.util.List[Message]) {
    this()
    this.underlying = new kafka.message.ByteBufferMessageSet(compressionEnabled, asBuffer(messages))
    this.buffer = underlying.buffer
    this.errorCode = underlying.errorCode
    this.deepIterate = underlying.deepIterate
  }

  def validBytes: Int = underlying.validBytes

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