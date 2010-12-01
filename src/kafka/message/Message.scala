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
import java.util.zip.CRC32
import java.util.UUID
import kafka.utils._

/**
 * Message byte offsets
 */
object Message {
  val CurrentMagicValue: Byte = 0
  val MagicOffset = 0
  val MagicLength = 1
  val CrcOffset = MagicOffset + MagicLength
  val CrcLength = 4
  val PayloadOffset = CrcOffset + CrcLength
  val HeaderSize = PayloadOffset
}

/**
 * A message. The format of an N byte message is the following:
 * 1 byte "magic" identifier to allow format changes
 * 4 byte CRC32 of the payload
 * N - 5 byte payload
 * 
 */
class Message(val buffer: ByteBuffer) {
  
  import kafka.message.Message._
    
  def this(checksum: Long, bytes: Array[Byte]) = {
    this(ByteBuffer.allocate(Message.HeaderSize + bytes.length))
    buffer.put(CurrentMagicValue)
    Utils.putUnsignedInt(buffer, checksum)
    buffer.put(bytes)
    buffer.rewind()
  }
  
  def this(bytes: Array[Byte]) = 
    this(Utils.crc32(bytes), bytes)
  
  def size: Int = buffer.limit
  
  def payloadSize: Int = size - HeaderSize
  
  def magic: Byte = buffer.get(MagicOffset)
  
  def checksum: Long = Utils.getUnsignedInt(buffer, CrcOffset)
  
  def payload: ByteBuffer = {
    var payload = buffer.duplicate
    payload.position(HeaderSize)
    payload = payload.slice()
    payload.limit(payloadSize) 
    payload.rewind()
    payload
  }
  
  def isValid: Boolean = 
    checksum == Utils.crc32(buffer.array, buffer.position + buffer.arrayOffset + PayloadOffset, payloadSize)
  
  override def toString(): String = 
    "message(magic = " + magic + ", crc = " + checksum + 
    ", payload = " + payload + ")"
  
  override def equals(any: Any): Boolean = {
    any match {
      case that: Message => size == that.size && checksum == that.checksum && payload == that.payload && magic == that.magic
      case _ => false
    }
  }
  
  override def hashCode(): Int = buffer.hashCode
  
}
