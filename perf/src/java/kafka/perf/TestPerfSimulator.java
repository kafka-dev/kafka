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

package kafka.perf;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Random;

import javax.management.MBeanServer;
import javax.management.ObjectName;

import joptsimple.OptionParser;
import joptsimple.OptionSet;
import kafka.perf.consumer.SimplePerfConsumer;
import kafka.perf.jmx.BrokerJmxClient;
import kafka.perf.producer.TestProducer;


public class TestPerfSimulator extends kafka.perf.PerfSimulator implements kafka.perf.KafkaSimulatorMXBean
{
	/*Command line parser options*/

	private static final String NUM_TOPIC = "numTopic";
	private static final String MSG_SIZE = "msgSize";

	private TestProducer [] producers;
	private SimplePerfConsumer [] consumers;

	public void startProducers() throws UnknownHostException
	{
		producers = new TestProducer[numProducer];
		Random random = new Random();
		String [] hosts = kafkaServersURL.split(",");
		for(int i = 0; i < numProducer; i++ )
		{
			String topic;
			String kafkaServerURL;
			if(numTopic >= numProducer)
				topic = "topic" +i;
			else
				topic = "topic" + random.nextInt(numTopic);

			if(hosts.length >= numProducer)
				kafkaServerURL = hosts[i];
			else
				kafkaServerURL = hosts[random.nextInt(hosts.length)];

			producers[i] = new TestProducer(topic,kafkaServerURL, kafkaServerPort, kafkaProducerBufferSize, connectionTimeOut,
					reconnectInterval, messageSize, InetAddress.getLocalHost().getHostAddress()+
					producerName +i, batchSize, numParts, compression);
		}

		// Start the threads
		for(int i = 0; i < numProducer; i++)
			producers[i].start();
	}

	public void startConsumers() throws UnknownHostException
	{
		System.out.println("Num consumers = " + numConsumers);
		consumers = new SimplePerfConsumer[numConsumers];
		Random random = new Random();
		String [] hosts = kafkaServersURL.split(",");
		for(int i = 0; i < numConsumers; i++ )
		{
			String topic;
			String kafkaServerURL;
			if(numTopic >= numConsumers)
				topic = "topic" +i;
			else
				topic = "topic" + random.nextInt(numTopic);

			if(hosts.length >= numConsumers)
				kafkaServerURL = hosts[i];
			else
				kafkaServerURL = hosts[random.nextInt(hosts.length)];

			consumers[i] = new SimplePerfConsumer(topic,kafkaServerURL, kafkaServerPort, kafkaProducerBufferSize, connectionTimeOut, reconnectInterval,
					fetchSize, InetAddress.getLocalHost().getHostAddress()+ consumerName +i, this.numParts);
		}

		// Start the threads
		for(int i = 0; i < numConsumers; i++)
			consumers[i].start();
	}

	public void startPerfRun() throws UnknownHostException, InterruptedException
	{
		startProducers();
		Thread.sleep(5000);
		startConsumers();
	}

	public String getMBytesSentPs()
	{
		if(producers == null ||producers.length == 0)
			return "";
		StringBuffer msg = new StringBuffer();
		for(int i = 0; i < numProducer -1; i++)
			msg.append(producers[i].getMBytesSentPs()  +",");
		msg.append(producers[numProducer -1].getMBytesSentPs());
		System.out.println(msg);
		return msg.toString();
	}

	  @Override
	  protected double getAvgMBytesSentPs()
	  {
	    if(producers == null ||producers.length == 0)
	      return 0;
	    double total = 0;
	    
	    for(int i = 0; i < numProducer -1; i++)
	      total = total + producers[i].getMBytesSentPs();
	    total = total +producers[numProducer -1].getMBytesSentPs();
	    return total /numProducer;
	   }
	
	  @Override
	  protected double getAvgMessagesSentPs()
	  {
	    if(producers == null ||producers.length == 0)
	      return 0;
	    double total = 0;
	    
	    for(int i = 0; i < numProducer -1; i++)
	      total = total + producers[i].getMessagesSentPs();
	    total = total +producers[numProducer -1].getMessagesSentPs();
	    return total /numProducer;
	   }
	  

	public String getMessagesSentPs()
	{
		if(producers == null ||producers.length == 0)
			return "";
		StringBuffer msg = new StringBuffer();
		for(int i = 0; i < numProducer -1; i++)
			msg.append(producers[i].getMessagesSentPs() +",");
		msg.append(producers[numProducer -1].getMessagesSentPs());
		System.out.println(msg);
		return msg.toString();
	}

	public String getProducers()
	{
		if(producers == null || producers.length == 0)
			return "";
		StringBuffer name = new StringBuffer();
		for(int i = 0; i < numProducer -1; i++)
			name.append(producers[i].getProducerName() +",");
		name.append(producers[numProducer -1].getProducerName());
		return name.toString();
	}

	public String getMBytesRecPs()
	{
		StringBuffer msg = new StringBuffer();
		for(int i = 0; i < numConsumers -1; i++)
			msg.append(consumers[i].getMBytesRecPs() +",");
		msg.append(consumers[numConsumers -1].getMBytesRecPs());
		//System.out.println(msg);
		return msg.toString();
	}

	  @Override
	  public double getAvgMBytesRecPs()
	  {
	    if(consumers == null ||consumers.length == 0)
	      return 0;
	
	    double total = 0;
	    for(int i = 0; i < numConsumers -1; i++)
	      total = total + consumers[i].getMBytesRecPs();
	
	   total = total + consumers[numConsumers -1].getMBytesRecPs();
	   
	   return total / numConsumers;
	  }
	
	  @Override
	  public double getAvgMessagesRecPs()
	  {
	    if(consumers == null ||consumers.length == 0)
	      return 0;
	
	    double total = 0;
	    for(int i = 0; i < numConsumers -1; i++)
	      total = total + consumers[i].getMessagesRecPs();
	
	   total = total + consumers[numConsumers -1].getMessagesRecPs();
	   
	   return total / numConsumers;
	  }


	public String getMessagesRecPs()
	{
		StringBuffer msg = new StringBuffer();
		for(int i = 0; i < numConsumers -1; i++)
			msg.append(consumers[i].getMessagesRecPs() +",");
		msg.append(consumers[numConsumers -1].getMessagesRecPs());
		//System.out.println(msg);
		return msg.toString();
	}

	public String getConsumers()
	{
		StringBuffer name = new StringBuffer();
		for(int i = 0; i < numConsumers -1; i++)
			name.append(consumers[i].getConsumerName() +",");
		name.append(consumers[numConsumers -1].getConsumerName());
		return name.toString();
	}

	public String getXaxisLabel()
	{
		if(NUM_PRODUCER.equals(xaxisLabel))
			return "Number of Producers";

		if(NUM_CONSUMER.equals(xaxisLabel))
			return "Number of Consumers";

		if(NUM_TOPIC.equals(xaxisLabel))
			return "Number of Topics";

		if(BATCH_SIZE.equals(xaxisLabel))
			return "Batch Size";

		return "";
	}

	public String getXAxisVal()
	{
		if(NUM_PRODUCER.equals(xaxisLabel))
			return ""+numProducer;

		if(NUM_CONSUMER.equals(xaxisLabel))
			return "" + numConsumers;

		if(NUM_TOPIC.equals(xaxisLabel))
			return ""+numTopic;

		if(BATCH_SIZE.equals(xaxisLabel))
			return "" + batchSize;

		return "";
	}

	protected OptionParser createParser()
	{
		OptionParser parser = super.createParser();

		/* optional values */
		parser.accepts(NUM_TOPIC, "number of topic").withOptionalArg().ofType(Integer.class);
		parser.accepts(MSG_SIZE, "message size").withOptionalArg().ofType(Integer.class);
		return parser;
	}

	protected void getOptions(OptionParser parser, String[] args) throws IOException
	{
		super.getOptions(parser, args);
		OptionSet options = parser.parse(args);

		if(options.hasArgument(NUM_TOPIC))
			numTopic = ((Integer)options.valueOf(NUM_TOPIC)).intValue();

		if(options.hasArgument(MSG_SIZE))
			messageSize = ((Integer)options.valueOf(MSG_SIZE)).intValue();
	}

	public static void main(String[] args) throws Exception {

		//create parser and get options

		BrokerJmxClient brokerStats = new BrokerJmxClient(kafkaServersURL, 9999, timeToRunMs);
		TestPerfSimulator sim = new TestPerfSimulator();
		sim.getOptions(sim.createParser(), args);
		sim.startPerfRun();
		
		PerfTimer timer = new PerfTimer( brokerStats, sim, numConsumers, numProducer,numParts, numTopic, 
				timeToRunMs,reportFileName, compression);
		timer.start();

		MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
		ObjectName mbeanName = new ObjectName("kafka.perf:type=Simulator");
		mbs.registerMBean(sim, mbeanName);
		while(true);
	}

	@Override
	protected String getKafkaServerLogDir() {
		return null;
	}

	@Override
	protected long getTotalBytesSent() {
		long bytes = 0;
		for(int i = 0;i < numProducer; i++)
			bytes += producers[i].getTotalBytesSent();
		return bytes;
	}
}
