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

package kafka.api

import java.nio._
import kafka.network._
import kafka.utils._
import kafka.api._

object MultiFetchRequest {
  def readFrom(buffer: ByteBuffer): MultiFetchRequest = {
    val count = buffer.getShort
    val fetches = new Array[FetchRequest](count)
    for(i <- 0 until fetches.length)
      fetches(i) = FetchRequest.readFrom(buffer)
    new MultiFetchRequest(fetches)
  }
}

class MultiFetchRequest(val fetches: Array[FetchRequest]) extends Request(RequestKeys.MultiFetch) {
  def writeTo(buffer: ByteBuffer) {
    buffer.putShort(fetches.length.toShort)
    for(fetch <- fetches)
      fetch.writeTo(buffer)
  }
  
  def sizeInBytes: Int = {
    var size = 2
    for(fetch <- fetches)
      size += fetch.sizeInBytes
    size
  }


  override def toString(): String = {
    val buffer = new StringBuffer
    for(fetch <- fetches) {
      buffer.append(fetch.toString)
      buffer.append(",")
    }
    buffer.toString
  }
}
