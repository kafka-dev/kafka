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

import java.net.URI
import java.io._
import joptsimple._
import kafka.message._
import kafka.producer._

/**
 * Interactive shell for producing messages from the command line
 */
object ProducerShell {

  def main(args: Array[String]) {
    
    val parser = new OptionParser
    val urlOpt = parser.accepts("server", "REQUIRED: The hostname of the server to connect to.")
                           .withRequiredArg
                           .describedAs("kafka://hostname:port")
                           .ofType(classOf[String])
    val topicOpt = parser.accepts("topic", "REQUIRED: The topic to consume from.")
                           .withRequiredArg
                           .describedAs("topic")
                           .ofType(classOf[String])
    
    val options = parser.parse(args : _*)
    
    for(arg <- List(urlOpt, topicOpt)) {
      if(!options.has(arg)) {
        System.err.println("Missing required argument \"" + arg + "\"") 
        parser.printHelpOn(System.err)
        System.exit(1)
      }
    }
    
    val url = new URI(options.valueOf(urlOpt))
    val topic = options.valueOf(topicOpt)
    val producer = new SimpleProducer(url.getHost, url.getPort, 64*1024, 10*1000, 100)
    
    val input = new BufferedReader(new InputStreamReader(System.in))
    var done = false
    while(!done) {
      val line = input.readLine()
      if(line == null) {
        done = true
      } else {
        val lineBytes = line.trim.getBytes()
        val messages = new ByteBufferMessageSet(new Message(lineBytes))
        producer.send(topic, messages)
        println("sent: " + line + " (" + messages.sizeInBytes + " bytes)")
      }
    }
    producer.close()
  }
}
