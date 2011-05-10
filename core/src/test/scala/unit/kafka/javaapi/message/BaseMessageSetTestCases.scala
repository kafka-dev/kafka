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

import junit.framework.Assert._
import org.scalatest.junit.JUnitSuite
import org.junit.Test
import kafka.message.Message
import kafka.utils.TestUtils

trait BaseMessageSetTestCases extends JUnitSuite {
  
  val messages = Array(new Message("abcd".getBytes()), new Message("efgh".getBytes()))
  
  def createMessageSet(messages: Seq[Message]): MessageSet

  @Test
  def testWrittenEqualsRead {
    import scala.collection.JavaConversions._
    val messageSet = createMessageSet(messages)
    TestUtils.checkEquals(asList(messages).iterator, messageSet.iterator)
  }

  @Test
  def testIteratorIsConsistent() {
    val m = createMessageSet(messages)
    // two iterators over the same set should give the same results
    TestUtils.checkEquals(m.iterator, m.iterator)
  }

  @Test
  def testSizeInBytes() {
    assertEquals("Empty message set should have 0 bytes.",
                 0L,
                 createMessageSet(Array[Message]()).sizeInBytes)
    assertEquals("Predicted size should equal actual size.", 
                 kafka.message.MessageSet.messageSetSize(messages).toLong,
                 createMessageSet(messages).sizeInBytes)
  }
}
