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

package kafka.tools

import kafka.consumer._

object GetOffsetShell {

  def main(args: Array[String]): Unit = {
    if(args.length != 6) {
      println("USAGE: " + GetOffsetShell.getClass.getName + " hostname port topic partition time number")
      System.exit(1)
    }
    val hostname = args(0)
    val port = args(1).toInt
    val topic = args(2)
    val partition = args(3).toInt
    var time = args(4).toLong
    val number = args(5).toInt
    val consumer = new SimpleConsumer(hostname, port, 10000, 100000)
    val offsets = consumer.getOffsetsBefore(topic, partition, time, number)
    println("get " + offsets.length + " results")
    for (offset <- offsets)
      println(offset)
  }
}