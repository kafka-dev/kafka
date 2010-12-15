package kafka.perf;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.net.InetAddress;
import java.net.UnknownHostException;

import javax.management.MBeanServer;
import javax.management.ObjectName;

import joptsimple.OptionParser;
import joptsimple.OptionSet;
import kafka.perf.consumer.SimplePerfConsumer;
import kafka.perf.jmx.BrokerJmxClient;
import kafka.perf.producer.PerfProducer;

public class KafkaPerfSimulator extends PerfSimulator implements KafkaSimulatorMXBean {

	protected static final String CONSUMER_KAFKA_SERVER = "consumerKafkaServer";
	protected static final String CONSUMER_KAFKA_PORT = "consumerKafkaPort";
	protected static final String CONSUMER_TOPIC = "consumerTopic";
	protected static final String PRODUCER_TOPIC = "producerTopic";
	protected static final String KAFKA_SERVER_LOG_DIR = "serverLogDir";
	
	private PerfProducer [] producers;
	private SimplePerfConsumer[] consumers;

	private String consumerKafkaServerURL;
	private int consumerKafkaServerPort;
	private String consumerTopic;
	private String producerTopic;
	private int fetchSize = 1024 * 1024; /*1MB*/;
	private String kafkaServerLogDir;

	public void startProducers() throws UnknownHostException
	{
		producers = new PerfProducer[numProducer];
		for(int i = 0; i < numProducer; i++ )
			producers[i] = new PerfProducer(producerTopic, kafkaServersURL, kafkaServerPort, kafkaProducerBufferSize, 
					connectionTimeOut, reconnectInterval, InetAddress.getLocalHost().getHostAddress()+ producerName +i, 
					batchSize, numParts, compression,
					fetchSize, consumerKafkaServerURL, consumerKafkaServerPort, consumerTopic);

		// Start the threads
		for(int i = 0; i < numProducer; i++)
			producers[i].start();
	}

	public void startConsumers() throws UnknownHostException
	{
		consumers = new SimplePerfConsumer[numConsumers];
		for(int i = 0; i < numConsumers; i++ )
			consumers[i] = new SimplePerfConsumer(producerTopic, kafkaServersURL, kafkaServerPort, kafkaProducerBufferSize, connectionTimeOut, reconnectInterval,
					fetchSize, InetAddress.getLocalHost().getHostAddress()+ consumerName +i, this.numParts);

		// Start the threads
		for(int i = 0; i < numConsumers; i++)
			consumers[i].start();
	}

	public void startPerfRun() throws InterruptedException, UnknownHostException {
		try {
			startProducers();
		} catch (UnknownHostException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		Thread.sleep(5000);
		startConsumers();
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		//create parser and get options

		BrokerJmxClient brokerStats = new BrokerJmxClient(kafkaServersURL, 9999, timeToRunMs);
		KafkaPerfSimulator sim = new KafkaPerfSimulator();
		try {
			sim.getOptions(sim.createParser(), args);
		} catch (IOException e) {
			System.err.println("Exception occurred while parsing arguments");
		}
		try {
			sim.startPerfRun();
		} catch (UnknownHostException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		} catch (InterruptedException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}

		PerfTimer timer = new PerfTimer(brokerStats, sim, numConsumers, numProducer,numParts, numTopic, 
				timeToRunMs, reportFileName, compression);
		timer.start();

		MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
		ObjectName mbeanName = null;
		try {
			mbeanName = new ObjectName("kafka.perf:type=Simulator");
			mbs.registerMBean(sim, mbeanName);
		} catch (Exception e) {
			System.err.println("Problem occured while registering perf mbean");
		} 
		while(true);

	}

	protected OptionParser createParser() {
		OptionParser parser = super.createParser();

		/* required arguments */
		parser.accepts(CONSUMER_TOPIC, "consumer topic").withRequiredArg().ofType(String.class);
		parser.accepts(PRODUCER_TOPIC, "producer topic").withRequiredArg().ofType(String.class);

		/* optional values */
		parser.accepts(CONSUMER_KAFKA_SERVER, "consumer kafka server").withOptionalArg().ofType(String.class);
		parser.accepts(CONSUMER_KAFKA_PORT, "consumer kafka server port").withOptionalArg().ofType(Integer.class);
		
		/* only makes sense if kafka server is localhost and compression ratio needs to be calculated */
		parser.accepts(KAFKA_SERVER_LOG_DIR, "kafka server log dir").withOptionalArg().ofType(String.class);
		
		return parser;
	}

	protected void getOptions(OptionParser parser, String[] args) throws IOException
	{
		super.getOptions(parser, args);
		System.out.println("Number of producers = " + numProducer);
		OptionSet options = parser.parse(args);

		if(!(options.hasArgument(CONSUMER_TOPIC) && options.hasArgument(PRODUCER_TOPIC)))
		{
			parser.printHelpOn(System.err);
			System.exit(1);
		}

		if(options.hasArgument(CONSUMER_TOPIC))
			consumerTopic = ((String)options.valueOf(CONSUMER_TOPIC));

		if(options.hasArgument(PRODUCER_TOPIC))
			producerTopic = ((String)options.valueOf(PRODUCER_TOPIC));

		if(producerTopic.equals(consumerTopic)) {
			if(!(options.hasArgument(CONSUMER_KAFKA_SERVER) && options.hasArgument(CONSUMER_KAFKA_PORT))) {
				System.err.println("Producer and consumer topics need to be different on the same Kafka server");
				System.exit(1);
			}
		}

		if(options.hasArgument(CONSUMER_KAFKA_SERVER))
			consumerKafkaServerURL = ((String)options.valueOf(CONSUMER_KAFKA_SERVER));
		else
			consumerKafkaServerURL = kafkaServersURL;

		if(options.hasArgument(CONSUMER_KAFKA_PORT))
			consumerKafkaServerPort = ((Integer)options.valueOf(CONSUMER_KAFKA_PORT)).intValue();
		else
			consumerKafkaServerPort = kafkaServerPort;
		
		if(options.hasArgument(KAFKA_SERVER_LOG_DIR))
			kafkaServerLogDir = ((String)options.valueOf(KAFKA_SERVER_LOG_DIR));
	}

	public String getConsumers() {
		StringBuffer name = new StringBuffer();
		for(int i = 0; i < numConsumers -1; i++)
			name.append(consumers[i].getConsumerName() +",");
		name.append(consumers[numConsumers -1].getConsumerName());
		return name.toString();
	}

	public String getMBytesRecPs() {
		// TODO Auto-generated method stub
		return null;
	}

	public String getMBytesSentPs() {
		if(producers == null ||producers.length == 0)
			return "";
		StringBuffer msg = new StringBuffer();
		for(int i = 0; i < numProducer -1; i++)
			msg.append(producers[i].getMBytesSentPs()  +",");
		msg.append(producers[numProducer -1].getMBytesSentPs());
		System.out.println(msg);
		return msg.toString();
	}

	public String getMessagesRecPs() {
		// TODO Auto-generated method stub
		return null;
	}

	public String getMessagesSentPs() {
		if(producers == null ||producers.length == 0)
			return "";
		StringBuffer msg = new StringBuffer();
		for(int i = 0; i < numProducer -1; i++)
			msg.append(producers[i].getMessagesSentPs() +",");
		msg.append(producers[numProducer -1].getMessagesSentPs());
		System.out.println(msg);
		return msg.toString();
	}

	public String getProducers() {
		// TODO Auto-generated method stub
		return null;
	}

	protected double getAvgMBytesSentPs() {
		if(producers == null ||producers.length == 0)
			return 0;
		double total = 0;

		for(int i = 0; i < numProducer-1; i++)
			total = total + producers[i].getMBytesSentPs();
		total = total +producers[numProducer-1].getMBytesSentPs();
		return total /numProducer;		
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

	protected double getAvgMessagesRecPs() {
		if(consumers == null ||consumers.length == 0)
			return 0;

		double total = 0;
		for(int i = 0; i < numConsumers -1; i++)
			total = total + consumers[i].getMessagesRecPs();

		total = total + consumers[numConsumers -1].getMessagesRecPs();
		return total / numConsumers;
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

	@Override
	protected long getTotalBytesSent() {
		long bytes = 0;
		for(int i = 0;i < numProducer; i++) {
			bytes += producers[i].getTotalBytesSent();
		}
		return bytes;
	}

	@Override
	protected String getKafkaServerLogDir() {
		return kafkaServerLogDir;
	}
}
