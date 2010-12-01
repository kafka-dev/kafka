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
import junit.framework.TestCase
import junit.framework.Assert._

class ByteBufferMessageSetTest extends BaseMessageSetTestCases {

  override def createMessageSet(messages: Seq[Message]): ByteBufferMessageSet = 
    new ByteBufferMessageSet(messages: _*)
  
  def testValidBytes() {
    val messages = new ByteBufferMessageSet(new Message("hello".getBytes()), new Message("there".getBytes()))
    val buffer = ByteBuffer.allocate(messages.sizeInBytes.toInt + 2)
    buffer.put(messages.buffer)
    buffer.putShort(4)
    val messagesPlus = new ByteBufferMessageSet(buffer)
    assertEquals("Adding invalid bytes shouldn't change byte count", messages.validBytes, messagesPlus.validBytes)
  }

  def testEquals() {
    val messages = new ByteBufferMessageSet(new Message("hello".getBytes()), new Message("there".getBytes()))
    val moreMessages = new ByteBufferMessageSet(new Message("hello".getBytes()), new Message("there".getBytes()))

    assertTrue(messages.equals(moreMessages))
  }
  
}
