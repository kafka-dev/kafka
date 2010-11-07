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

import java.io._
object MakeRandomStrings {
  
  def main(args: Array[String]): Unit = {
    if(args.length < 2) {
      println("USAGE: " + MakeRandomStrings.getClass.getName + " length width file")
      System.exit(1)
    }
    
    val length = args(0).toInt
    val width = args(1).toInt
    val writer = new BufferedWriter(new FileWriter(args(2)))
    
    for(i <- 0 until length) {
      writer.write(TestUtils.randomString(width))
      writer.write("\n")
      if(i % 100000 == 0)
        println(i)
    }
  }
  
}
