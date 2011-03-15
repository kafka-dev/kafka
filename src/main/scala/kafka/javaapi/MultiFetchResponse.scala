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

package kafka.javaapi

import message.ByteBufferMessageSet
import kafka.utils.IteratorTemplate
import java.nio.ByteBuffer
import Implicits._

class MultiFetchResponse(buffer: ByteBuffer, numSets: Int) extends IteratorTemplate[ByteBufferMessageSet] {
  val underlyingBuffer = ByteBuffer.wrap(buffer.array)
    // this has the side effect of setting the initial position of buffer correctly
  val errorCode = underlyingBuffer.getShort

  val underlying = new kafka.api.MultiFetchResponse(underlyingBuffer, numSets)

  override def toString() = underlying.toString

  protected def makeNext(): ByteBufferMessageSet = {
    if(underlying.iterator.hasNext)
      underlying.iterator.next
    else
      return allDone
  }

}