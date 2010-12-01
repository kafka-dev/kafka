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
import scala.collection.mutable
import kafka.utils.IteratorTemplate
import kafka.message._

class MultiFetchResponse(buffer: ByteBuffer, numSets: Int) extends IteratorTemplate[ByteBufferMessageSet] {

  private val messageSets = new mutable.ListBuffer[ByteBufferMessageSet]
  
  for(i <- 0 until numSets) {
    val size = buffer.getInt()
    val errorCode: Int = buffer.getShort()
    val copy = buffer.slice()
    val payloadSize = size - 2
    copy.limit(payloadSize)
    buffer.position(buffer.position + payloadSize)
    messageSets += new ByteBufferMessageSet(copy, errorCode)
  }
 
  private val iterator = messageSets.iterator
 
  override def toString() = this.messageSets.toString

  protected def makeNext(): ByteBufferMessageSet = {
    if(iterator.hasNext)
      iterator.next
    else
      return allDone
  }
}
