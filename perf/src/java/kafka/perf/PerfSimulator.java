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
import joptsimple.OptionParser;
import joptsimple.OptionSet;
import kafka.perf.consumer.PerfConsumer;
import kafka.perf.producer.Producer;

public abstract class PerfSimulator {
	/*Command line parser options*/

	protected static final String NUM_PRODUCER = "numProducer";
	protected static final String NUM_CONSUMER = "numConsumer";
	protected static final String NUM_TOPIC = "numTopic";
	protected static final String NUM_PARTS = "numParts";
	protected static final String TEST_TIME = "time";
	protected static final String KAFKA_SERVER = "kafkaServer";
	protected static final String KAFKA_PORT = "kafkaPort";
	protected static final String MSG_SIZE = "msgSize";
	protected static final String FETCH_SIZE = "fetchSize";
	protected static final String XAXIS = "xaxis";
	protected static final String COMPRESSION= "compression";
	protected static final String BATCH_SIZE = "batchSize";

	/* Default values */
	protected static int numProducer = 1;
	protected static int numTopic = 1;
	protected static int numParts = 1;
	protected static int numConsumers = 1;
	protected static long timeToRunMs = 60000L * 1; 

	protected static String kafkaServersURL = "";
	protected static int kafkaServerPort = 9092;
	protected final static int kafkaProducerBufferSize = 64*1024;
	protected final static int connectionTimeOut = 100000;
	protected final static int reconnectInterval = 10000;
	protected static int messageSize = 200;
	protected static int fetchSize = 1024 *1024; /*1MB*/
	protected static int batchSize = 200;
	final static String producerName = "-Producer-";
	final static String consumerName = "-Consumer-";
	protected static String reportFileName = "";
	protected static String xaxisLabel = "";
	protected static Boolean compression = false;
	protected final static String REPORT_FILE = "reportFile";

	private Producer[] producers;
	private PerfConsumer[] consumers;

	protected OptionParser createParser()
	{
		OptionParser parser = new OptionParser();

		/* required arguments */
		parser.accepts(KAFKA_SERVER, "kafka server url").withRequiredArg().ofType(String.class);
		parser.accepts(KAFKA_PORT, "kafka server port").withRequiredArg().ofType(Integer.class);
		parser.accepts(REPORT_FILE, "report file name").withRequiredArg().ofType(String.class);
		parser.accepts(XAXIS, "report xaxis").withRequiredArg().ofType(String.class);

		/* optional values */
		parser.accepts(NUM_PRODUCER, "number of producers").withOptionalArg().ofType(Integer.class);
		parser.accepts(NUM_CONSUMER, "number of consumers").withOptionalArg().ofType(Integer.class);
		parser.accepts(NUM_PARTS, "number of partitions").withOptionalArg().ofType(Integer.class);
		parser.accepts(TEST_TIME, "time to run tests").withOptionalArg().ofType(Integer.class);
		parser.accepts(FETCH_SIZE, "fetch size").withOptionalArg().ofType(Integer.class);
		parser.accepts(COMPRESSION, "compression").withOptionalArg().ofType(Boolean.class);
		parser.accepts(BATCH_SIZE, "batch size").withOptionalArg().ofType(Integer.class);

		return parser;
	}

	protected void getOptions(OptionParser parser, String[] args) throws IOException
	{
		OptionSet options = parser.parse(args);

		if(!(options.hasArgument(KAFKA_SERVER) && options.hasArgument(REPORT_FILE) 
				&&  options.hasArgument(XAXIS) && options.hasArgument(KAFKA_PORT))) {
			parser.printHelpOn(System.err);
			System.exit(1);
		}

		kafkaServersURL = (String)options.valueOf(KAFKA_SERVER);
		kafkaServerPort = (Integer)options.valueOf(KAFKA_PORT);
		reportFileName= (String)options.valueOf(REPORT_FILE);
		xaxisLabel = (String)options.valueOf(XAXIS);

		System.out.println("server: " + kafkaServersURL + "port: " + kafkaServerPort + " report: " +
				reportFileName + " xaxisLabel : " + xaxisLabel);

		if(options.hasArgument(NUM_PRODUCER))
			numProducer = ((Integer)options.valueOf(NUM_PRODUCER)).intValue();
		if(options.hasArgument(NUM_CONSUMER))
			numConsumers = ((Integer)options.valueOf(NUM_CONSUMER)).intValue();

		if(options.hasArgument(NUM_PARTS))
			numParts = ((Integer)options.valueOf(NUM_PARTS)).intValue();

		if(options.hasArgument(TEST_TIME))
			timeToRunMs = ((Integer)options.valueOf(TEST_TIME)).intValue() * 60 * 1000;

		if(options.hasArgument(FETCH_SIZE))
			fetchSize = ((Integer)options.valueOf(FETCH_SIZE)).intValue();

		if(options.hasArgument(COMPRESSION))
			compression = ((Boolean)options.valueOf(COMPRESSION)).booleanValue();

		if(options.hasArgument(BATCH_SIZE))
			batchSize = ((Integer)options.valueOf(BATCH_SIZE)).intValue();

		System.out.println("numTopic: " + numTopic);
	}

	protected String getXaxisLabel()
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

	protected String getXAxisVal()
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

	protected double getAvgMBytesRecPs() {
		if(consumers == null ||consumers.length == 0)
			return 0;

		double total = 0;
		for(int i = 0; i < numConsumers -1; i++)
			total = total + consumers[i].getMBytesRecPs();

		total = total + consumers[numConsumers -1].getMBytesRecPs();

		return total / numConsumers;
	}

	protected double getAvgMBytesSentPs() {
		if(producers == null ||producers.length == 0)
			return 0;
		double total = 0;

		for(int i = 0; i < numProducer -1; i++)
			total = total + producers[i].getMBytesSentPs();
		total = total +producers[numProducer -1].getMBytesSentPs();
		return total /numProducer;		
	}

	protected double getAvgMessagesRecPs() {
		if(consumers == null ||consumers.length == 0)
			return 0;

		double total = 0;
		for(int i = 0; i < numConsumers -1; i++)
			total = total + consumers[i].getMessagesRecPs();

		total = total + consumers[numConsumers -1].getMessagesRecPs();

		return total / numConsumers;

	}

	protected double getAvgMessagesSentPs() {
		if(producers == null ||producers.length == 0)
			return 0;
		double total = 0;

		for(int i = 0; i < numProducer -1; i++)
			total = total + producers[i].getMessagesSentPs();
		total = total +producers[numProducer -1].getMessagesSentPs();
		return total /numProducer;
	}
	
	protected abstract long getTotalBytesSent();
	protected abstract String getKafkaServerLogDir();
	
}