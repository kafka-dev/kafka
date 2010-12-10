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

package kafka.common

import kafka.consumer._
import kafka.message.InvalidMessageException

/**
 * A bi-directional mapping between error codes and exceptions x  
 */
object ErrorMapping {
  val NO_ERROR = 0
  val OFFSET_OUT_OF_RANGE_CODE = 1
  val INVALID_MESSAGE_CODE = 2
  val WRONG_PARTITION_CODE = 3
  
  private val exceptionToCode = 
    Map[Class[Exception], Int](
      classOf[OffsetOutOfRangeException].asInstanceOf[Class[Exception]] -> OFFSET_OUT_OF_RANGE_CODE,
      classOf[InvalidMessageException].asInstanceOf[Class[Exception]] -> INVALID_MESSAGE_CODE,
      classOf[WrongPartitionException].asInstanceOf[Class[Exception]] -> WRONG_PARTITION_CODE
    ).withDefaultValue(0)
  
  /* invert the mapping */
  private val codeToException = 
    (Map[Int, Class[Exception]]() ++ exceptionToCode.iterator.map(p => (p._2, p._1))).withDefaultValue(classOf[UnknownException])
  
  def codeFor(exception: Class[Exception]): Int = exceptionToCode(exception)
  
  def maybeThrowException(code: Int) =
    if(code > 0)
      throw codeToException(code).newInstance()
}

class WrongPartitionException(message: String) extends RuntimeException(message) {
  def this() = this(null)  
}
