package kafka.perf;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;

import kafka.perf.jmx.BrokerJmxClient;

public class PerfTimer extends Thread
{
	private final long timeToRun;
	private final BrokerJmxClient brokerStats;
	private final PerfSimulator perfSim;
	private final int numConsumers, numProducer,numParts, numTopic;
	private final String reportFile;
	private final boolean compression;

	public PerfTimer(BrokerJmxClient brokerStats,
			PerfSimulator perfSim, int numConsumers, 
			int numProducer, int numParts, int numTopic,
			long timeToRun,
			String fileName,
			boolean compression)
	{
		this.timeToRun = timeToRun;
		this.brokerStats = brokerStats;
		this.perfSim = perfSim;
		this.numConsumers = numConsumers;
		this.numProducer = numProducer;
		this.numParts = numParts;
		this.numTopic = numTopic;
		reportFile = fileName;
		this.compression = compression;
	}

	public void printMBDataStats() throws Exception
	{
		File mbDataFile = new File(reportFile + "/MBdata.csv");
		System.out.println("Printing MB/s stats to " + mbDataFile.getAbsolutePath());
		boolean witeHeader = !mbDataFile.exists();
		FileWriter fstream = new FileWriter(mbDataFile, true);
		BufferedWriter writer = new BufferedWriter(fstream);
		if(witeHeader)
			writer.write(perfSim.getXaxisLabel() + ",consumer-MB/sec,total-consumer-MB/sec,producer-MB/sec, total-producer-MB/sec\n");
		writer.write(perfSim.getXAxisVal() + "," + perfSim.getAvgMBytesRecPs() + "," + (numConsumers *perfSim.getAvgMBytesRecPs()) +
				"," + perfSim.getAvgMBytesSentPs()  + "," + (perfSim.getAvgMBytesSentPs() * numProducer ));

		writer.newLine();
		writer.close();
		fstream.close();
	}

	public void printMessageDataStats() throws Exception
	{
		System.out.println("Printing messages/s stats");
		File file = new File(reportFile + "/NumMessage.csv");
		boolean witeHeader = !file.exists();
		FileWriter fstream = new FileWriter(file, true);
		BufferedWriter writer = new BufferedWriter(fstream);
		if(witeHeader)
			writer.write(perfSim.getXaxisLabel() + ",consumer-messages/sec,total-consumer-messages/sec,producer-messages/sec, total-producer-messages/sec\n");
		writer.write(perfSim.getXAxisVal() + "," + perfSim.getAvgMessagesRecPs() + "," + (numConsumers *perfSim.getAvgMessagesRecPs()) +
				"," + perfSim.getAvgMessagesSentPs()  + "," + (perfSim.getAvgMessagesSentPs() * numProducer) );

		writer.newLine();
		writer.close();
		fstream.close();
	}

	public void printCompressionRatio() throws Exception 
	{
		if(perfSim.kafkaServersURL.equals("localhost") && (perfSim.getKafkaServerLogDir() != null)) {
			File logDir = new File(perfSim.getKafkaServerLogDir());
			long totalLogLength = 0L;
			if(logDir.isDirectory()) {
				File[] files = logDir.listFiles();
				for(int i = 0; i < files.length; i++)
					totalLogLength += files[i].length();
			}else 
				totalLogLength += logDir.length();
			System.out.println("Log length = " + totalLogLength);
			File file = new File(reportFile + "/CompressionRatio.csv");
			boolean witeHeader = !file.exists();
			FileWriter fstream = new FileWriter(file, true);
			BufferedWriter writer = new BufferedWriter(fstream);
			if(witeHeader)
				writer.write(perfSim.getXaxisLabel() + "Compression Ratio\n");
			writer.write(perfSim.getXAxisVal() + "," + (perfSim.getTotalBytesSent()/(double)totalLogLength));

			writer.newLine();
			writer.close();
			fstream.close();
		}
	}

	public void printReport() throws Exception
	{
		String header = "#consumers, #of producers, #of partitions, #of topic, " +
		"consumer mess/sec,consumer MB/sec, producer mess/sec,producer MB/sec, broker MB write/sec, broker MB read/sec";
		String data = numConsumers+ "," + numProducer + "," + numParts+ "," + numTopic + "," +
		perfSim.getAvgMessagesRecPs() + "," +
		perfSim.getAvgMBytesRecPs() + "," +
		perfSim.getAvgMessagesSentPs() + "," +
		perfSim.getAvgMBytesSentPs();
		//                       brokerStats.getBrokerStats();

		System.out.println(header);
		System.out.println(data);
		printMessageDataStats();
		printMBDataStats();
		if(compression)
			printCompressionRatio();
	}

	public void run() {
		try
		{
			Thread.sleep(timeToRun);
		}
		catch (InterruptedException e)
		{
			e.printStackTrace();
		}

		try
		{
			printReport();
		}
		catch (Exception e)
		{
			e.printStackTrace();
		}
		System.exit(0);
	}
}