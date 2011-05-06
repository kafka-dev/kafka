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

import java.util.concurrent.atomic.AtomicBoolean
import java.io.File
import kafka.utils.{Utils, nonthreadsafe}
import kafka.message.Message
import java.nio.channels.{FileChannel, WritableByteChannel}

/**
 * An on-disk message set. The set can be opened either mutably or immutably. Mutation attempts
 * will fail on an immutable message set. An optional limit and offset can be applied to the message set
 * which will control the offset into the file and the effective length into the file from which
 * messages will be read
 */
@nonthreadsafe
class FileMessageSet private[message](private[message] val channel: FileChannel,
                                      private[message] val offset: Long,
                                      private[message] val limit: Long,
                                      val mutable: Boolean,
                                      val needRecover: AtomicBoolean) extends MessageSet {
  private val underlying = new kafka.message.FileMessageSet(channel, offset, limit, mutable, needRecover)

  /**
   * Create a file message set with no limit or offset
   */
  def this(channel: FileChannel, mutable: Boolean) =
    this(channel, 0, Long.MaxValue, mutable, new AtomicBoolean(false))

  /**
   * Create a file message set with no limit or offset
   */
  def this(file: File, mutable: Boolean) =
    this(Utils.openChannel(file, mutable), mutable)

  /**
   * Create a file message set with no limit or offset
   */
  def this(channel: FileChannel, mutable: Boolean, needRecover: AtomicBoolean) =
    this(channel, 0, Long.MaxValue, mutable, needRecover)

  /**
   * Create a file message set with no limit or offset
   */
  def this(file: File, mutable: Boolean, needRecover: AtomicBoolean) =
    this(Utils.openChannel(file, mutable), mutable, needRecover)


  /**
   * Return a message set which is a view into this set starting from the given offset and with the given size limit.
   */
  def read(readOffset: Long, size: Long): MessageSet = new kafka.javaapi.message.MessageSet {

    val messageSet = underlying.read(readOffset, size)
    val messageSetIter = messageSet.iterator

    override def writeTo(channel: WritableByteChannel, offset: Long, maxSize: Long): Long =
      messageSet.writeTo(channel, offset, maxSize)

    override def iterator: java.util.Iterator[Message] = new java.util.Iterator[Message] {
      override def hasNext(): Boolean = messageSetIter.hasNext
      override def next(): Message = messageSetIter.next
      override def remove = throw new UnsupportedOperationException("remove operation on MessageSet iterator is " +
      "not supported.")
    }

    override def sizeInBytes: Long = messageSet.sizeInBytes
  }

  /**
   * Write some of this set to the given channel, return the ammount written
   */
  def writeTo(destChannel: WritableByteChannel, writeOffset: Long, size: Long): Long =
    underlying.writeTo(destChannel, writeOffset, size)

  /**
   * Get an iterator over the messages in the set
   */
  override def iterator: java.util.Iterator[Message] = new java.util.Iterator[Message] {
    val underlyingIterator = underlying.iterator
    override def hasNext(): Boolean = underlyingIterator.hasNext
    override def next(): Message = underlyingIterator.next
    override def remove = throw new UnsupportedOperationException("remove operation on MessageSet iterator is " +
      "not supported.")
  }

  /**
   * The number of bytes taken up by this file set
   */
  def sizeInBytes(): Long = underlying.sizeInBytes

  /**
    * The high water mark
    */
  def highWaterMark(): Long = underlying.highWaterMark

  def checkMutable(): Unit = {
    underlying.checkMutable
  }

  /**
   * Append this message to the message set
   */
  def append(messages: MessageSet): Unit = {
    val scalaMessageSet = new kafka.message.MessageSet {
      override def writeTo(channel: WritableByteChannel, offset: Long, maxSize: Long): Long =
        messages.writeTo(channel, offset, maxSize)

      override def iterator: Iterator[Message] = new Iterator[Message] {
        override def hasNext(): Boolean = messages.iterator.hasNext
        override def next(): Message = messages.iterator.next
      }

      override def sizeInBytes: Long = messages.sizeInBytes

    }
    underlying.append(scalaMessageSet)
  }

  /**
   * Commit all written data to the physical disk
   */
  def flush() = {
    underlying.flush
  }

  /**
   * Close this message set
   */
  def close() = {
    underlying.close
  }

  /**
   * Recover log up to the last complete entry. Truncate off any bytes from any incomplete messages written
   */
  def recover(): Long = {
    underlying.recover
  }
}