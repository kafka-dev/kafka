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

package kafka

import message.Message
import org.apache.log4j.{Logger, PropertyConfigurator}
import serializer.SerDeser

object TestKafkaAppender {

  private val logger = Logger.getLogger(TestKafkaAppender.getClass)
  
  def main(args:Array[String]) {
    
    if(args.length < 1) {
      println("USAGE: " + TestKafkaAppender.getClass.getName + " log4j_config")
      System.exit(1)
    }

    try {
      PropertyConfigurator.configure(args(0))
    } catch {
      case e: Exception => System.err.println("KafkaAppender could not be initialized ! Exiting..")
      e.printStackTrace()
      System.exit(1)
    }

    for(i <- 1 to 10)
      logger.info("test")    
  }
}

class AppenderStringSerializer extends SerDeser[AnyRef] {
  def toEvent(message: Message):AnyRef = message.toString
  def toMessage(event: AnyRef):Message = {
    new Message(event.asInstanceOf[String].getBytes)
  }
  def getTopic(event: AnyRef): String = null
}

