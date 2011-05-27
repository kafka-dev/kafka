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
  val CurrentMagicValue: Byte = 1
  val MagicOffset = 0
  val MagicLength = 1
  val AttributeOffset = MagicOffset + MagicLength
  val AttributeLength = 1
  
  def CrcOffset(magic:Byte): Int = magic match {
    case 0 => MagicOffset + MagicLength
    case _ => AttributeOffset + AttributeLength
  }
  
  val CrcLength = 4
  def PayloadOffset(magic:Byte): Int = CrcOffset(magic) + CrcLength
  def HeaderSize(magic:Byte): Int = PayloadOffset(magic)
  val MinHeaderSize = HeaderSize(0);
  // Attribute masks follow
  val COMPRESSION_CODEC_MASK:Int = 0x03  // 2 bits to hold the compression codec. 0 is reserved to indicate no compression
  
  
  // Defaults follow
  val NO_COMPRESSION:Int = 0
}

/**
 * A message. The format of an N byte message is the following:
 * 1 byte "magic" identifier to allow format changes
 * 1 byte "attributes" identifier to allow annotations on the message independent of the version (e.g. compression enabled, type of codec used)
 * 4 byte CRC32 of the payload
 * N - 8 byte payload
 * 
 */
class Message(val buffer: ByteBuffer) {
  
  import kafka.message.Message._
    
  
  private def this(checksum: Long, bytes: Array[Byte], compressionCodec:Int) = {
    this(ByteBuffer.allocate(Message.HeaderSize(Message.CurrentMagicValue) + bytes.length))
    buffer.put(CurrentMagicValue)
    var attributes:Byte = 0
    if (compressionCodec >0) {
      attributes =  (attributes | (Message.COMPRESSION_CODEC_MASK & compressionCodec)).toByte     
    }
    buffer.put(attributes)
    Utils.putUnsignedInt(buffer, checksum)
    buffer.put(bytes)
    buffer.rewind()
  }
  def this(checksum:Long, bytes:Array[Byte]) = this(checksum, bytes, Message.NO_COMPRESSION)
  
  def this(bytes: Array[Byte], compressionCodec:Int) = {
    //Note: we're not crc-ing the attributes header, so we're susceptible to bit-flipping there
    this(Utils.crc32(bytes), bytes, compressionCodec)
  }
  def this(bytes:Array[Byte]) = this(bytes, Message.NO_COMPRESSION)
  
  def size: Int = buffer.limit
  
  def payloadSize: Int = size - HeaderSize(magic)
  
  def magic: Byte = buffer.get(MagicOffset)
  
  def attributes: Byte = buffer.get(AttributeOffset)
  
  def compressionCodec:Int = {
    buffer.get(AttributeOffset) & COMPRESSION_CODEC_MASK;
  }
  
  def isCompressed:Boolean = magic match {
    case 0 => false
    case _ => compressionCodec > 0
  }
  
  def checksum: Long = Utils.getUnsignedInt(buffer, CrcOffset(magic))
  
  def payload: ByteBuffer = {
    var payload = buffer.duplicate
    payload.position(HeaderSize(magic))
    payload = payload.slice()
    payload.limit(payloadSize)
    payload.rewind()
    payload
  }
  
  def isValid: Boolean = 
    checksum == Utils.crc32(buffer.array, buffer.position + buffer.arrayOffset + PayloadOffset(magic), payloadSize)
     
  def serializedSize: Int = 4 /* int size*/ + buffer.limit
   
  def serializeTo(serBuffer:ByteBuffer) = {
    serBuffer.putInt(buffer.limit)
    serBuffer.put(buffer.duplicate)
  }

  override def toString(): String = 
    "message(magic = " + magic + ", attributes = " + attributes + ", crc = " + checksum + 
    ", payload = " + payload + ")"
  
  override def equals(any: Any): Boolean = {
    any match {
      case that: Message => size == that.size && attributes == that.attributes && checksum == that.checksum && payload == that.payload && magic == that.magic
      case _ => false
    }
  }
  
  override def hashCode(): Int = buffer.hashCode
  
}
