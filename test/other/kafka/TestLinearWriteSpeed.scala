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
import java.nio._
import java.nio.channels._

object TestLinearWriteSpeed {

  def main(args: Array[String]): Unit = {
    if(args.length < 3) {
      println("USAGE: java TestLinearWriteSpeed size_in_mb write_size num_files")
      System.exit(1)
    }
    val mbToWrite = args(0).toInt
    val bufferSize = args(1).toInt
    val numFiles = args(2).toInt
    val buffer = ByteBuffer.allocate(bufferSize)
    while(buffer.hasRemaining)
      buffer.put(123.asInstanceOf[Byte])
    
    val channels = new Array[FileChannel](numFiles)
    for(i <- 0 until numFiles) {
      val file = File.createTempFile("kafka-test", ".dat")
      file.deleteOnExit()
      channels(i) = new RandomAccessFile(file, "rw").getChannel()
    }
    
    val begin = System.currentTimeMillis
    for(i <- 0 until mbToWrite * 1024 * 1024 / bufferSize) {
      buffer.rewind()
      channels(i % numFiles).write(buffer)
    }
    val ellapsedSecs = (System.currentTimeMillis - begin) / 1000.0
    System.out.println(mbToWrite / ellapsedSecs + " MB per sec")
  }
  
  def blah() {
    var count = 0
    /*
    val stream = new KafkaMessageStream(null, null, 1, 2, 100, null, 5, true)
    for(message <- stream) {
      count += 1
      if(count % 1000 == 0)
        println(count)
    }
    */
    1
  }
  
}
